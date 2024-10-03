// EventProcessingMediator.java
package com.example.demo.service;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
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
        private final ChangeEventServiceInterface changeEventService;
        private final ResumeTokenService resumeTokenService;
        private final TpsCalculator tpsCalculator; // TPS calculator instance
        private final PrometheusMetricsConfig metricsConfig; //
        private ExecutorService[] executors;
        private final AtomicInteger threadCounter = new AtomicInteger();

        @Value("${spring.threadpool.nums}")
        private int nums;

        @Value("${spring.lifecycle.timeout-per-shutdown-phase:30s}") // Configurable shutdown timeout duration
        private String shutdownTimeoutString;

        private long shutdownTimeout; // Timeout value in seconds

        @Autowired
        public EventProcessingMediator(@Qualifier("changeEventService") ChangeEventServiceInterface changeEventService,
                        ResumeTokenService resumeTokenService,
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

                // Parse the shutdown timeout string to seconds
                this.shutdownTimeout = Duration.parse("PT" + shutdownTimeoutString.replaceAll("[^0-9]", "") + "S")
                                .getSeconds();
                // Initialize the executor with the daemon thread factory
                // Initialize a pool of executor services with daemon threads
                executors = new ExecutorService[nums];
                ThreadFactory daemonThreadFactory = runnable -> {
                        Thread thread = new Thread(runnable);
                        thread.setDaemon(true);
                        thread.setName("Thread-" + threadCounter.getAndIncrement());
                        return thread;
                };
                for (int i = 0; i < nums; i++) {
                        executors[i] = Executors.newSingleThreadExecutor(daemonThreadFactory);
                }
        }

        public ChangeStreamIterable<Document> changeStreamIterator(BsonDocument resumeToken) {
                return changeEventService.changeStreamIterator(resumeToken);
        }

        /**
         * Use Spring @Retryable to auto retry network relate exceptions in MongoDB
         * The NoPrimaryException in MongoDB Driver client is handled by native dirver
         * already.
         * 
         * @param event
         */

        @Retryable(value = { MongoTimeoutException.class, MongoSocketReadException.class,
                        MongoSocketWriteException.class, MongoCommandException.class,
                        MongoWriteConcernException.class }, maxAttemptsExpression = "${spring.mongodb.retry.maxattempts}", backoff = @Backoff(delayExpression = "${spring.mongodb.retry.initialdelayms}"))
        public void processEvent(ChangeStreamDocument<Document> event) {
                String currentThreadName = Thread.currentThread().getName();
                long startTimeMillis = System.currentTimeMillis();
                LOGGER.info("Thread " + currentThreadName + "  is correctly processing change: {}" + event);

                long eventMillis;
                Document fullDocument = event.getFullDocument();
                if (fullDocument != null && fullDocument.containsKey("date")) {
                        eventMillis = ((java.util.Date) fullDocument.get("date")).getTime();
                } else {
                        eventMillis = event.getClusterTime().getTime() * 1000;
                }

                // Record the event for TPS calculation
                tpsCalculator.recordEvent(currentThreadName);
                double eventLag = (startTimeMillis - eventMillis);
                // Record event lag for the current thread
                metricsConfig.eventLagPerThread().labels(currentThreadName).set(eventLag);

                // Call ChangeEventService to process the change event
                changeEventService.processChange(event);

                //TODO: at-least once, resume need de-dup logic or process need to be idempotent
                // Save the resume token after processing
                BsonDocument resumeToken = event.getResumeToken();
                if (resumeToken != null) {
                        resumeTokenService.saveResumeToken(resumeToken, currentThreadName);
                }

                double tps = tpsCalculator.calculateTps(currentThreadName);
                metricsConfig.tpsPerThread().labels(currentThreadName).set(tps);
                // Record the processing duration
                // Calculate and observe the processing duration
                long durationMillis = System.currentTimeMillis() - startTimeMillis; // Duration in milliseconds
                double durationSeconds = durationMillis / 1000.0; // Convert duration to seconds

                metricsConfig.eventProcessDuration().observe(durationSeconds);
                metricsConfig.p99ProcessingTime().observe(durationMillis);
        }

        public BsonDocument getLatestResumeToken() {
                // Delegate to ResumeTokenService to get the latest resume token
                return resumeTokenService.getLatestResumeToken();
        }

        public void shutdown() {
                LOGGER.info("Shutdown requested, closing change stream...");
                for (ExecutorService executor : executors) {
                        if (executor != null) {
                                executor.shutdown();
                                try {
                                        if (!executor.awaitTermination(shutdownTimeout, TimeUnit.SECONDS)) {
                                                executor.shutdownNow();
                                                if (!executor.awaitTermination(shutdownTimeout, TimeUnit.SECONDS)) {
                                                        LOGGER.error("Executor service did not terminate gracefully.");
                                                }
                                        }
                                } catch (InterruptedException ie) {
                                        executor.shutdownNow();
                                        Thread.currentThread().interrupt();
                                }
                        }
                }
        }

        /**
         * MongoNotPrimaryException is not need to handle manually
         */
        public void changeStreamProcessWithRetry() {
                BsonDocument resumeToken = getLatestResumeToken();
                ChangeStreamIterable<Document> changeStream = changeStreamIterator(resumeToken);

                // special logic, make sure the same playerID event will be handled in the same
                // thread for ever.
                changeStream.forEach(event -> {
                        LOGGER.info("Received event, starting handling {}", event);
                        try {
                                Document fullDocument = event.getFullDocument();
                                int playerID = fullDocument.getInteger("playerID");
                                // Determine which executor to use based on playerID
                                int executorIndex = playerID % nums;

                                LOGGER.info("evnet {}, playerID {}, executor index {}", event, playerID, executorIndex);
                                // Submit the task to the corresponding executor
                                CompletableFuture.runAsync(() -> processEvent(event), executors[executorIndex])
                                                .exceptionally(ex -> {
                                                        // Log the exception that occurred inside processEvent
                                                        LOGGER.error("Exception occurred while processing event: {}",
                                                                        event, ex);
                                                        return null; // return null since exceptionally requires a
                                                                     // return value
                                                });
                        } catch (Exception e) {
                                LOGGER.error("Non-retryable exception occurred while processing event: {}", event, e);
                        }
                });
        }
}