package com.example.demo;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.example.demo.service.EventProcessingMediator;

import io.prometheus.client.exporter.HTTPServer;

public class DemoApplicationTest {

        @Mock
        private EventProcessingMediator mediator;

        @InjectMocks
        private DemoApplication demoApplication;

        @Mock
        private HTTPServer httpServer;

        @BeforeEach
        public void setUp() {
                MockitoAnnotations.openMocks(this);
        }

        @Test
        public void testStartHttpServer() {
                demoApplication.startHttpServer();
                verify(mediator, never()).changeStreamProcessWithRetry();
        }

        @Test
        public void testCloseConnection() {
                demoApplication.closeConnection();
                verify(mediator, times(1)).shutdown();
        }
}
