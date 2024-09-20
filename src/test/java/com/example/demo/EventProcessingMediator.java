package com.example.demo;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import com.example.demo.metrics.PrometheusMetricsConfig;
import com.example.demo.metrics.TpsCalculator;
import com.example.demo.service.ChangeEventService;
import com.example.demo.service.EventProcessingMediator;
import com.example.demo.service.ResumeTokenService;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

class EventProcessingMediatorTest {

        @Mock
        private ChangeEventService changeEventService;

        @Mock
        private ResumeTokenService resumeTokenService;

        @Mock
        private TpsCalculator tpsCalculator;

        @Mock
        private PrometheusMetricsConfig metricsConfig;

        @Mock
        private Gauge mockGauge; // Mock Gauge object

        @Mock
        private Gauge.Child mockGaugeChild; // Mock Gauge.Child object

        @Mock
        private Histogram mockHistogram; // Mock Histogram object

        @Mock
        private Histogram.Child mockHistogramChild; // Mock Histogram.Child object

        @Mock
        private Summary mockSummary; // Mock Summary object

        @Mock
        private Summary.Child mockSummaryChild; // Mock Summary.Child object

        @InjectMocks
        private EventProcessingMediator mediator;

        @BeforeEach
        void setUp() {
                MockitoAnnotations.openMocks(this);

                // Set up mock returns for PrometheusMetricsConfig
                when(metricsConfig.eventLagPerThread()).thenReturn(mockGauge);
                when(metricsConfig.tpsPerThread()).thenReturn(mockGauge);
                when(metricsConfig.eventProcessDuration()).thenReturn(mockHistogram);
                when(metricsConfig.p99ProcessingTime()).thenReturn(mockSummary);

                // Mocking behavior of Gauge's labels method to return Gauge.Child
                when(mockGauge.labels(anyString())).thenReturn(mockGaugeChild);
                when(mockHistogram.labels(anyString())).thenReturn(mockHistogramChild);
                when(mockSummary.labels(anyString())).thenReturn(mockSummaryChild);

                // Mocking behavior of Histogram's and Summary's observe methods
                doNothing().when(mockHistogramChild).observe(anyDouble());
                doNothing().when(mockSummaryChild).observe(anyDouble());
        }

        void testProcessChangeEvent() {
                // Use inline mocking for final classes
                ChangeStreamDocument<Document> mockEvent = mock(ChangeStreamDocument.class);
                Document mockFullDocument = new Document("date", new java.util.Date());
                when(mockEvent.getFullDocument()).thenReturn(mockFullDocument);
                when(mockEvent.getClusterTime()).thenReturn(null);
                BsonDocument mockResumeToken = new BsonDocument();
                when(mockEvent.getResumeToken()).thenReturn(mockResumeToken);

                mediator.processChangeEvent(mockEvent, 1);

                // Verify that methods are called as expected
                verify(tpsCalculator).recordEvent(anyString());
                verify(changeEventService).processChange(mockEvent, 1);
                verify(resumeTokenService).saveResumeToken(mockResumeToken, 1);
                verify(mockGauge).labels(anyString()); // Verify labels method is called on the mocked gauge
                verify(mockGaugeChild).set(anyDouble()); // Verify that set() was called on Gauge.Child
                verify(mockHistogramChild).observe(anyDouble()); // Verify observe() was called on Histogram.Child
                verify(mockSummaryChild).observe(anyDouble()); // Verify observe() was called on Summary.Child
        }
}
