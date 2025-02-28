package com.example.demo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

        // ✅ Force stubbed behavior
        doNothing().when(mediator).startChangeStreamListeners();
    }

    @Test
    public void testInit() throws InterruptedException {
        // ✅ Ensure `startChangeStreamListeners()` is actually stubbed before running
        verify(mediator, never()).startChangeStreamListeners();

        // ✅ Manually trigger `@PostConstruct`
        demoApplication.init();

        // ✅ Introduce a small delay to ensure the asynchronous method is actually executed
        Thread.sleep(50); // Adjust timing if needed

        // ✅ Verify that `startChangeStreamListeners()` was actually called
        verify(mediator, times(1)).startChangeStreamListeners();
    }

    @Test
    public void testCloseConnection() {
        demoApplication.closeConnection();
        verify(mediator, times(1)).shutdown();
    }
}
