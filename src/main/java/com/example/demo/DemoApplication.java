package com.example.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.example.demo.service.EventProcessingMediator;

import io.prometheus.client.exporter.HTTPServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@SpringBootApplication
@Configurable
public class DemoApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplication.class);

    @Value("${prometheus.server.port:8081}") // Default port for metrics
    private int metricsPort;

    @Autowired
    private EventProcessingMediator mediator;

    private HTTPServer httpServer; // Add HTTPServer instance

    public DemoApplication() {
    }

    @PostConstruct
    public void init() {
        startHttpServer(); // Start the HTTP server for metrics
        startChangeStreamListener();
    }

    public void startHttpServer() {
        try {
            // Start the Prometheus HTTP server on the specified port
            httpServer = new HTTPServer(metricsPort);
            LOGGER.info("Prometheus metrics server started on port {}", metricsPort);
        } catch (Exception e) {
            LOGGER.error("Error starting Prometheus HTTP server: {}", e.getMessage());
        }
    }

    public void startChangeStreamListener() {
        Thread changeStreamThread = new Thread(() -> {
            // Run the blocking change stream processing logic
            mediator.changeStreamProcessWithRetry();
        });
        changeStreamThread.setDaemon(true); // Set the thread as a daemon thread
        changeStreamThread.start(); // Start the thread
        LOGGER.info("Change stream listener started in a daemon thread");
    }

    @PreDestroy
    public void closeConnection() {
        try {
            if (httpServer != null) {
                httpServer.stop(); // Stop the HTTP server if it's running
                LOGGER.info("Prometheus metrics server stopped");
            }
            mediator.shutdown();
        } catch (Exception e) {
            LOGGER.error("Error closing MongoDB connection: {}", e);
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
