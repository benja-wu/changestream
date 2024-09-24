// DemoApplicationTest.java
package com.example.demo;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.demo.service.EventProcessingMediator;

import io.prometheus.client.exporter.HTTPServer;

@ExtendWith(MockitoExtension.class)
class DemoApplicationTest {

        @Mock
        private EventProcessingMediator mediator;

        @Mock
        private HTTPServer httpServer;

        @InjectMocks
        private DemoApplication demoApplication;

        @BeforeEach
        void setUp() {
                MockitoAnnotations.openMocks(this);
        }

        @Test
        void testStartHttpServer() {
                // We don't start the actual HTTP server, so no stubbing needed for `httpServer`
                demoApplication.startHttpServer();

                verify(mediator, never()).changeStreamProcessWithRetry();
        }

        @Test
        void testStartChangeStreamListener() {
                // Verify that the change stream listener can be started without threads
                demoApplication.startChangeStreamListener();

                verify(mediator, times(1)).changeStreamProcessWithRetry();
        }
}
