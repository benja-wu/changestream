package com.example.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.example.demo.service.EventProcessingMediator;

import io.prometheus.client.exporter.HTTPServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@SpringBootApplication
public class DemoApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplication.class);

    @Value("${prometheus.server.port:8081}")
    private int metricsPort;

    @Autowired
    private EventProcessingMediator mediator;

    private HTTPServer httpServer;

  
    @PostConstruct
    public void init() {
        startHttpServer();
        startChangeStreamListener();
    }

    protected void startHttpServer() {
        try {
            httpServer = new HTTPServer(metricsPort);
            LOGGER.info("Prometheus metrics server started on port {}", metricsPort);
        } catch (Exception e) {
            LOGGER.error("Error starting Prometheus HTTP server: {}", e.getMessage());
        }
    }

    private void startChangeStreamListener() {
        Thread changeStreamThread = new Thread(() -> {
            mediator.startChangeStreamListeners();
        });
        changeStreamThread.setDaemon(true);
        changeStreamThread.start();
        LOGGER.info("Change stream listener started in a daemon thread");
    }

    @PreDestroy
    public void closeConnection() {
        try {
            if (httpServer != null) {
                httpServer.stop();
                LOGGER.info("Prometheus metrics server stopped");
            }
            mediator.shutdown();
        } catch (Exception e) {
            LOGGER.error("Error closing MongoDB connection: {}", e);
        }
    }

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(DemoApplication.class, args);
        EventProcessingMediator mediator = context.getBean(EventProcessingMediator.class);
        mediator.init();
    }

}

