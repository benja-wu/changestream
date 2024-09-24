// EventProcessingMediatorTest.java
package com.example.demo;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import com.example.demo.metrics.PrometheusMetricsConfig;
import com.example.demo.metrics.TpsCalculator;
import com.example.demo.service.ChangeEventService;
import com.example.demo.service.EventProcessingMediator;
import com.example.demo.service.ResumeTokenService;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

class EventProcessingMediatorTest {

        @Mock
        private ChangeEventService changeEventService;

        @Mock
        private ResumeTokenService resumeTokenService;

        @Mock
        private TpsCalculator tpsCalculator;

        @Mock
        private PrometheusMetricsConfig metricsConfig;

        @InjectMocks
        private EventProcessingMediator mediator;

        @BeforeEach
        void setUp() {
                MockitoAnnotations.openMocks(this);

                // Manually set @Value properties
                ReflectionTestUtils.setField(mediator, "nums", 2); // Set thread pool size
                ReflectionTestUtils.setField(mediator, "maxAttempts", 3); // Set max retry attempts
                ReflectionTestUtils.setField(mediator, "initialDelayMS", 100); // Set initial delay for retries

                // Initialize the mediator's thread pool and metrics
                mediator.init();

                // Mock all necessary metrics configurations to avoid NullPointerException
                when(metricsConfig.eventLagPerThread()).thenReturn(mock(io.prometheus.client.Gauge.class));
                when(metricsConfig.eventLagPerThread().labels(any(String.class)))
                                .thenReturn(mock(io.prometheus.client.Gauge.Child.class));
                when(metricsConfig.tpsPerThread()).thenReturn(mock(io.prometheus.client.Gauge.class));
                when(metricsConfig.tpsPerThread().labels(any(String.class)))
                                .thenReturn(mock(io.prometheus.client.Gauge.Child.class));
        }

        @Test
        void testChangeStreamProcessWithRetry() {
                BsonDocument resumeToken = BsonDocument.parse("{'_data': 'test'}");
                ChangeStreamIterable<Document> changeStreamIterable = mock(ChangeStreamIterable.class);
                ChangeStreamDocument<Document> changeStreamDocument = mock(ChangeStreamDocument.class);
                BsonTimestamp bsonTimestamp = new BsonTimestamp(1625140800, 1);

                when(resumeTokenService.getLatestResumeToken()).thenReturn(resumeToken);
                when(changeEventService.changeStreamIterator(resumeToken)).thenReturn(changeStreamIterable);
                when(changeStreamDocument.getClusterTime()).thenReturn(bsonTimestamp);

                // Mock the change stream forEach behavior
                doAnswer(invocation -> {
                        mediator.processEvent(changeStreamDocument);
                        return null;
                }).when(changeStreamIterable).forEach(any());

                mediator.changeStreamProcessWithRetry();

                verify(changeEventService, times(1)).changeStreamIterator(resumeToken);
                verify(resumeTokenService, times(1)).getLatestResumeToken();
        }

        @Test
        void testProcessEventWithRetryHandling() {
                ChangeStreamDocument<Document> changeStreamDocument = mock(ChangeStreamDocument.class);
                BsonTimestamp bsonTimestamp = new BsonTimestamp(1625140800, 1);

                // Configure the mock to return a BsonTimestamp when getClusterTime() is called
                when(changeStreamDocument.getClusterTime()).thenReturn(bsonTimestamp);

                // Mock processChange to throw an exception the first time and return 0 the next
                // time
                when(changeEventService.processChange(any(ChangeStreamDocument.class)))
                                .thenThrow(new RuntimeException("Simulated Exception"))
                                .thenReturn(0); // Next call succeeds

                // Mock Prometheus metrics to avoid null issues
                when(metricsConfig.eventLagPerThread()).thenReturn(mock(io.prometheus.client.Gauge.class));
                when(metricsConfig.eventLagPerThread().labels(any(String.class)))
                                .thenReturn(mock(io.prometheus.client.Gauge.Child.class));
                when(metricsConfig.tpsPerThread()).thenReturn(mock(io.prometheus.client.Gauge.class));
                when(metricsConfig.tpsPerThread().labels(any(String.class)))
                                .thenReturn(mock(io.prometheus.client.Gauge.Child.class));

                try {
                        mediator.processEvent(changeStreamDocument);
                } catch (RuntimeException e) {
                        // Handle the simulated exception to allow the test to proceed
                        System.err.println("Caught expected exception: " + e.getMessage());
                }

                verify(changeEventService, atLeastOnce()).processChange(changeStreamDocument);
        }
}
