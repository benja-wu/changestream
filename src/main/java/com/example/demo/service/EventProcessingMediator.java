// EventProcessingMediator.java
package com.example.demo.service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.demo.metrics.PrometheusMetricsConfig;
import com.example.demo.metrics.TpsCalculator;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import jakarta.annotation.PostConstruct;

/**
 * EventProcessingMediator uses ChangeEventService and ResumeTokenService to
 * listen to one mongoDB
 * collection's change event, it's resumable and enable max attempt retry logic
 */
@Service
@Configurable
public class EventProcessingMediator {

        private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessingMediator.class);
        private final ChangeEventService changeEventService;
        private final ResumeTokenService resumeTokenService;
        private final TpsCalculator tpsCalculator; // TPS calculator instance
        private final PrometheusMetricsConfig metricsConfig; //
        public ExecutorService executor;

        @Value("${spring.threadpool.nums}")
        private Integer nums;

        @Value("${spring.mongodb.retry.maxattempts}")
        private Integer maxAttempts;

        @Value("${spring.mongodb.retry.initialdelayms}")
        private Integer initialDelayMS;

        @Autowired
        public EventProcessingMediator(ChangeEventService changeEventService, ResumeTokenService resumeTokenService,
                        PrometheusMetricsConfig metricsConfig, TpsCalculator tpsCalculator) {
                this.changeEventService = changeEventService;
                this.resumeTokenService = resumeTokenService;
                this.metricsConfig = metricsConfig; // Inject metrics configuration
                this.tpsCalculator = tpsCalculator; // In
        }

        @PostConstruct
        public void init() {
                metricsConfig.totalEventsHandled();
                metricsConfig.totalEventsHandledSuccessfully();
                metricsConfig.eventLagPerThread();
                metricsConfig.eventProcessDuration();
                metricsConfig.tpsPerThread();
                metricsConfig.p99ProcessingTime();
                // Use a custom thread factory to create daemon threads
                ThreadFactory daemonThreadFactory = runnable -> {
                        Thread thread = new Thread(runnable);
                        thread.setDaemon(true); // Set the thread as daemon
                        return thread;
                };

                // Initialize the executor with the daemon thread factory
                executor = Executors.newFixedThreadPool(nums, daemonThreadFactory);
        }

        public ChangeStreamIterable<Document> changeStreamIterator(BsonDocument resumeToken) {
                return changeEventService.changeStreamIterator(resumeToken);
        }

        public void processEvent(ChangeStreamDocument<Document> event) {
                String currentThreadName = Thread.currentThread().getName();
                long startTime = System.currentTimeMillis();
                LOGGER.info("Thread " + currentThreadName + "  is correctly processing change: {}" + event);

                long eventMillis;
                Document fullDocument = event.getFullDocument();
                if (fullDocument != null && fullDocument.containsKey("date")) {
                        eventMillis = ((java.util.Date) fullDocument.get("date")).getTime();
                        LOGGER.info("Date field in milliseconds: {}", eventMillis);
                } else {
                        eventMillis = event.getClusterTime().getTime() * 1000;
                }

                // Record the event for TPS calculation
                tpsCalculator.recordEvent(currentThreadName);
                double eventLag = (System.currentTimeMillis() - eventMillis);
                // Record event lag for the current thread
                metricsConfig.eventLagPerThread().labels(currentThreadName).set(eventLag);

                // Call ChangeEventService to process the change event
                changeEventService.processChange(event);

                // Save the resume token after processing
                BsonDocument resumeToken = event.getResumeToken();
                if (resumeToken != null) {
                        resumeTokenService.saveResumeToken(resumeToken, currentThreadName);
                }

                double tps = tpsCalculator.calculateTps(currentThreadName);
                metricsConfig.tpsPerThread().labels(currentThreadName).set(tps);
                // Record the processing duration
                // Calculate and observe the processing duration
                long durationMillis = System.currentTimeMillis() - startTime; // Duration in milliseconds
                double durationSeconds = durationMillis / 1000.0; // Convert duration to seconds

                metricsConfig.eventProcessDuration().observe(durationSeconds);
                metricsConfig.p99ProcessingTime().observe(durationMillis);
        }

        public BsonDocument getLatestResumeToken() {
                // Delegate to ResumeTokenService to get the latest resume token
                return resumeTokenService.getLatestResumeToken();
        }

        private void retrySleep(long delay) {
                try {
                        TimeUnit.MILLISECONDS.sleep(delay);
                } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOGGER.error("Retry sleep interrupted", ie);
                }
        }

        public void shutdown() {
                LOGGER.info("Shutdown requested, closing change stream...");
                if (executor != null) {
                        executor.shutdown();
                        try {
                                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                                        executor.shutdownNow();
                                        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                                                LOGGER.error("Executor service did not terminate gracefully.");
                                        }
                                }
                        } catch (InterruptedException ie) {
                                executor.shutdownNow();
                                Thread.currentThread().interrupt();
                        }
                }
        }

        /**
         * MongoNotPrimaryException is not need to handle manually
         */
        public void changeStreamProcessWithRetry() {
                BsonDocument resumeToken = getLatestResumeToken();
                ChangeStreamIterable<Document> changeStream = changeStreamIterator(resumeToken);

                // Start the change stream with or without a resume token
                changeStream.forEach(event -> {
                        // Get event's unique _id
                        LOGGER.info("get one event and start handle {}, first try", event);

                        executor.submit(() -> {
                                int attempt = 0;
                                long delay = initialDelayMS;
                                while (attempt < maxAttempts) {
                                        try {
                                                processEvent(event);
                                                break;
                                        } catch (MongoTimeoutException | MongoSocketReadException
                                                        | MongoSocketWriteException
                                                        | MongoCommandException
                                                        | MongoWriteConcernException e) {
                                                // Retry on specific exceptions that require manual
                                                // handling
                                                attempt++;
                                                LOGGER.warn("Attempt {} failed for event {}. Retrying in {} ms...",
                                                                attempt, event, delay, e);
                                                retrySleep(delay);
                                                delay *= 2;
                                        } catch (Exception e) {
                                                // For other exceptions, do not retry and log the error
                                                LOGGER.error("Non-retryable exception occurred while processing event: {}",
                                                                event, e);
                                        }
                                }
                        });
                });
        }
}