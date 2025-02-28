package com.example.demo;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.Mock;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import com.example.demo.metrics.PrometheusMetricsConfig;
import com.example.demo.metrics.TpsCalculator;
import com.example.demo.service.BusinessTask;
import com.example.demo.service.ResumeTokenService;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

class BusinessTaskTest {

    @Mock
    private ResumeTokenService resumeTokenService;

    @Mock
    private TpsCalculator tpsCalculator;

    @Mock
    private PrometheusMetricsConfig metricsConfig;

    @Mock
    private MongoCollection<Document> collection;

    @Mock
    private MongoClient mongoClient;

    private BusinessTask businessTask;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // Create an instance of BusinessTask using an anonymous subclass
        businessTask = new BusinessTask(resumeTokenService, tpsCalculator, metricsConfig, "testCollection", mongoClient) {
            @Override
            protected int processChange(ChangeStreamDocument<Document> event) {
                return 0; // Simulate successful processing
            }
        };

        businessTask.setCollection(collection);

        // Mock Prometheus Metrics
        Gauge mockGauge = mock(Gauge.class);
        when(metricsConfig.getEventLagPerThread()).thenReturn(mockGauge);
        when(mockGauge.labels(anyString())).thenReturn(mock(Gauge.Child.class));

        Gauge tpsGauge = mock(Gauge.class);
        when(metricsConfig.getTpsPerThread()).thenReturn(tpsGauge);
        when(tpsGauge.labels(anyString())).thenReturn(mock(Gauge.Child.class));

        Histogram mockHistogram = mock(Histogram.class);
        when(metricsConfig.getEventProcessDuration()).thenReturn(mockHistogram);
        when(metricsConfig.getP99ProcessingTime()).thenReturn(mockHistogram); // Fix: Ensure this is not null

        doNothing().when(mockHistogram).observe(anyDouble());
    }

    @Test
    void testStartProcessing_Success() {
        // Arrange: Mock change stream event
        ChangeStreamDocument<Document> event = mock(ChangeStreamDocument.class);
        when(event.getClusterTime()).thenReturn(new BsonTimestamp(1000, 1));
        when(event.getResumeToken()).thenReturn(new BsonDocument());

        // Act & Assert
        assertDoesNotThrow(() -> businessTask.startProcessing("testThread", event));

        // Verify TPS recording and metric updates
        verify(tpsCalculator, times(1)).recordEvent("testCollection", "testThread");
        verify(metricsConfig, times(1)).incrementTotalEventsHandled();
        verify(metricsConfig, times(1)).incrementTotalEventsHandledSuccessfully();
        verify(resumeTokenService, times(1)).saveResumeToken(any(), any(), eq("testThread"), eq("testCollection"));
    }

    @Test
    void testStartProcessing_ExceptionHandling() {
        // Arrange: Mock change stream event
        ChangeStreamDocument<Document> event = mock(ChangeStreamDocument.class);
        when(event.getClusterTime()).thenReturn(new BsonTimestamp(1000, 1));
        when(event.getResumeToken()).thenReturn(new BsonDocument());

        // Simulate exception in `processChange`
        BusinessTask failingTask = new BusinessTask(resumeTokenService, tpsCalculator, metricsConfig, "testCollection", mongoClient) {
            @Override
            protected int processChange(ChangeStreamDocument<Document> event) {
                throw new RuntimeException("Processing error");
            }
        };
        failingTask.setCollection(collection);

        // Act & Assert: Expect exception
        Exception exception = assertThrows(RuntimeException.class, () -> failingTask.startProcessing("testThread", event));
        assertEquals("Processing error", exception.getMessage());

        // Ensure resumeTokenService was not called
        verify(resumeTokenService, never()).saveResumeToken(any(), any(), anyString(), anyString());
    }

    @Test
    void testChangeStreamIterator_WithResumeToken() {
        // Arrange: Mock ChangeStreamIterable
        ChangeStreamIterable<Document> mockChangeStream = mock(ChangeStreamIterable.class);
        when(collection.watch()).thenReturn(mockChangeStream);
        when(mockChangeStream.resumeAfter(any(BsonDocument.class))).thenReturn(mockChangeStream); // Fix: Return valid mock

        // Act
        BsonDocument resumeToken = new BsonDocument();
        ChangeStreamIterable<Document> result = businessTask.changeStreamIterator(resumeToken);

        // Assert
        assertNotNull(result);
        verify(collection, times(1)).watch();
        verify(mockChangeStream, times(1)).resumeAfter(resumeToken);
    }

    @Test
    void testChangeStreamIterator_WithoutResumeToken() {
        // Arrange: Mock ChangeStreamIterable
        ChangeStreamIterable<Document> mockChangeStream = mock(ChangeStreamIterable.class);
        when(collection.watch()).thenReturn(mockChangeStream);

        // Act
        ChangeStreamIterable<Document> result = businessTask.changeStreamIterator(null);

        // Assert
        assertNotNull(result);
        verify(collection, times(1)).watch();
        verify(mockChangeStream, never()).resumeAfter(any(BsonDocument.class));
    }

    @Test
    void testChangeStreamIterator_ThrowsException_WhenCollectionNotSet() {
        // Arrange
        BusinessTask uninitializedTask = new BusinessTask(resumeTokenService, tpsCalculator, metricsConfig, "testCollection", mongoClient) {
            @Override
            protected int processChange(ChangeStreamDocument<Document> event) {
                return 0;
            }
        };

        // Act & Assert
        Exception exception = assertThrows(IllegalStateException.class, () -> {
            uninitializedTask.changeStreamIterator(null);
        });

        assertTrue(exception.getMessage().contains("Collection has not been set"));
    }
}
