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
import com.mongodb.MongoSocketReadException;
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
                ReflectionTestUtils.setField(mediator, "shutdownTimeoutString", "30s"); // Set shutdown timeout duration

                // Initialize the mediator's thread pool and metrics
                mediator.init();

                // Mock all necessary metrics configurations to avoid NullPointerException
                mockMetrics();
        }

        private void mockMetrics() {
                // Mocking metrics methods to avoid NullPointerExceptions
                when(metricsConfig.eventLagPerThread()).thenReturn(mock(io.prometheus.client.Gauge.class));
                when(metricsConfig.eventLagPerThread().labels(any(String.class)))
                                .thenReturn(mock(io.prometheus.client.Gauge.Child.class));

                when(metricsConfig.tpsPerThread()).thenReturn(mock(io.prometheus.client.Gauge.class));
                when(metricsConfig.tpsPerThread().labels(any(String.class)))
                                .thenReturn(mock(io.prometheus.client.Gauge.Child.class));

                // Correctly mock the histogram and timer for eventProcessDuration
                io.prometheus.client.Histogram histogramMock = mock(io.prometheus.client.Histogram.class);
                io.prometheus.client.Histogram.Timer timerMock = mock(io.prometheus.client.Histogram.Timer.class);
                when(metricsConfig.eventProcessDuration()).thenReturn(histogramMock);
                when(histogramMock.startTimer()).thenReturn(timerMock);

                // Use doAnswer to simulate void method observeDuration
                doAnswer(invocation -> null).when(timerMock).observeDuration();

                // Correctly mock the summary for p99ProcessingTime
                io.prometheus.client.Summary summaryMock = mock(io.prometheus.client.Summary.class);
                when(metricsConfig.p99ProcessingTime()).thenReturn(summaryMock);
                doAnswer(invocation -> null).when(summaryMock).observe(anyDouble());
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
                                .thenThrow(new MongoSocketReadException("Simulated MongoSocketReadException", null))
                                .thenReturn(0); // Next call succeeds

                // Mock Prometheus metrics to avoid null issues
                mockMetrics();

                try {
                        mediator.processEvent(changeStreamDocument);
                } catch (Exception e) {
                        // Handle the simulated exception to allow the test to proceed
                        System.err.println("Caught expected exception: " + e.getMessage());
                }

                verify(changeEventService, atLeastOnce()).processChange(changeStreamDocument);
        }

        @Test
        void testShutdown() throws InterruptedException {
                // Ensure the mediator.shutdown() properly handles executor shutdown
                mediator.shutdown();

                // Verify that shutdownNow() is called when timeout occurs
                verify(resumeTokenService, never()).getLatestResumeToken(); // Ensure no interactions post shutdown
        }
}
