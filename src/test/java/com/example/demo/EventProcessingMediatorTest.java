package com.example.demo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import com.example.demo.metrics.PrometheusMetricsConfig;
import com.example.demo.metrics.TpsCalculator;
import com.example.demo.service.ChangeEventService;
import com.example.demo.service.EventProcessingMediator;
import com.example.demo.service.ResumeTokenService;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

public class EventProcessingMediatorTest {

        @Mock
        private ChangeEventService changeEventService;

        @Mock
        private ResumeTokenService resumeTokenService;

        @Mock
        private PrometheusMetricsConfig metricsConfig;

        @Mock
        private TpsCalculator tpsCalculator;

        @Mock
        private Gauge.Child gaugeChild;

        @InjectMocks
        private EventProcessingMediator mediator;

        @BeforeEach
        public void setUp() {
                // Initialize the mocks
                MockitoAnnotations.openMocks(this);

                // Mock the Gauge and its Child
                Gauge mockGauge = mock(Gauge.class);
                when(metricsConfig.eventLagPerThread()).thenReturn(mockGauge);
                when(mockGauge.labels(any(String.class))).thenReturn(gaugeChild);

                // Mock the tpsPerThread Gauge
                Gauge tpsGauge = mock(Gauge.class);
                when(metricsConfig.tpsPerThread()).thenReturn(tpsGauge);
                when(tpsGauge.labels(any(String.class))).thenReturn(gaugeChild);

                // Mock the Histogram for event process duration
                Histogram mockHistogram = mock(Histogram.class);
                Histogram.Child mockHistogramChild = mock(Histogram.Child.class);
                when(metricsConfig.eventProcessDuration()).thenReturn(mockHistogram);
                when(mockHistogram.labels(any(String.class))).thenReturn(mockHistogramChild);
                doNothing().when(mockHistogramChild).observe(anyDouble());

                // Mock the Summary for p99ProcessingTime
                Summary mockSummary = mock(Summary.class);
                Summary.Child mockSummaryChild = mock(Summary.Child.class);
                when(metricsConfig.p99ProcessingTime()).thenReturn(mockSummary);
                when(mockSummary.labels(any(String.class))).thenReturn(mockSummaryChild);
                doNothing().when(mockSummaryChild).observe(anyDouble());

                   // Mock the totalEventsHandled Counter
                Counter totalEventsCounter = mock(Counter.class);
                when(metricsConfig.totalEventsHandled()).thenReturn(totalEventsCounter);
                doNothing().when(totalEventsCounter).inc();

                // Mock the totalEventsHandledSuccessfully Counter
                Counter totalEventsSuccessCounter = mock(Counter.class);
                when(metricsConfig.totalEventsHandledSuccessfully()).thenReturn(totalEventsSuccessCounter);
                doNothing().when(totalEventsSuccessCounter).inc();
        }

        @Test
        public void testChangeStreamIterator() {
                // Correctly mock ChangeStreamIterable
                ChangeStreamIterable<Document> changeStreamIterable = mock(ChangeStreamIterable.class);
                when(changeEventService.changeStreamIterator(any(BsonDocument.class))).thenReturn(changeStreamIterable);

                mediator.changeStreamIterator(new BsonDocument());

                verify(changeEventService, times(1)).changeStreamIterator(any(BsonDocument.class));
        }

        @Test
        public void testProcessEvent() {
                // Mock a ChangeStreamDocument and set up its behavior
                ChangeStreamDocument<Document> event = mock(ChangeStreamDocument.class);
                BsonTimestamp bsonTimestamp = new BsonTimestamp(1000, 1);
                when(event.getClusterTime()).thenReturn(bsonTimestamp);
                when(event.getResumeToken()).thenReturn(new BsonDocument());

                mediator.processEvent(event);

                verify(resumeTokenService, times(1)).saveResumeToken(any(), any(), anyString());
        }

        @Test
        public void testProcessEventThrowsException() {
                // Arrange
                ChangeStreamDocument<Document> event = mock(ChangeStreamDocument.class);
                BsonTimestamp bsonTimestamp = new BsonTimestamp(1000, 1);
                when(event.getClusterTime()).thenReturn(bsonTimestamp);
                when(event.getResumeToken()).thenReturn(new BsonDocument());

                // Mock changeEventService to throw an exception when processChange is called
                doThrow(new RuntimeException("Simulated exception")).when(changeEventService).processChange(any());

                // Create a simple single-thread executor for this test
                ExecutorService executor = Executors.newSingleThreadExecutor();

                // Act
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        mediator.processEvent(event);
                }, executor).exceptionally(ex -> {
                        // Assert
                        // This block will execute when the exception is thrown and caught
                        System.out.println("Caught exception: " + ex.getMessage());
                        // Verifying that the exception is indeed handled
                        assertTrue(ex.getMessage().contains("Simulated exception"));
                        return null; // Return null as required by exceptionally
                });

                // Ensure the CompletableFuture is completed to properly test exception handling
                future.join(); // Waits for the completion of the async task

                // Shutdown executor after use to avoid resource leak
                executor.shutdown();
        }
}
