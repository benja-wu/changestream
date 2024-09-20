// EventProcessingMediator.java
package com.example.demo.service;

import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.example.demo.metrics.PrometheusMetricsConfig;
import com.example.demo.metrics.TpsCalculator;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import jakarta.annotation.PostConstruct;

@Service
public class EventProcessingMediator {
        private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessingMediator.class);
        private final ChangeEventService changeEventService;
        private final ResumeTokenService resumeTokenService;
        private final TpsCalculator tpsCalculator; // TPS calculator instance
        private final PrometheusMetricsConfig metricsConfig; //

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
        }

        public void processChangeEvent(ChangeStreamDocument<Document> event, int threadId) {
                String currentThreadName = Thread.currentThread().getName();
                LOGGER.info("in process change event");
                long startTime = System.currentTimeMillis();
                System.out.println("Thread " + currentThreadName + " (assigned as " + threadId
                                + ") is correctly processing change: {}" + event);

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
                changeEventService.processChange(event, threadId);

                // Save the resume token after processing
                BsonDocument resumeToken = event.getResumeToken();
                if (resumeToken != null) {
                        resumeTokenService.saveResumeToken(resumeToken, threadId);
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
}
