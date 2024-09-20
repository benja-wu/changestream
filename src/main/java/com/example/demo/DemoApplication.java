package com.example.demo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.example.demo.service.EventProcessingMediator;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;

import io.prometheus.client.exporter.HTTPServer;
import jakarta.annotation.PostConstruct;

@SpringBootApplication
@Configurable
public class DemoApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplication.class);

    @Value("${spring.threadpool.nums}")
    public Integer nums;

    @Value("${server.port:8081}") // Default port for metrics
    private int metricsPort;

    @Autowired
    private EventProcessingMediator mediator;

    @Autowired
    @Qualifier("changestreamCollection")
    private MongoCollection<Document> changeStreamCollection; // Inject MongoCollection from MongoConfig

    private ExecutorService executorService;
    private HTTPServer httpServer; // Add HTTPServer instance

    public DemoApplication() {
    }

    @PostConstruct
    public void init() {

        executorService = Executors.newFixedThreadPool(nums);
        startHttpServer(); // Start the HTTP server for metrics
        startChangeStreamListener();
    }

    private void startHttpServer() {
        try {
            // Start the Prometheus HTTP server on the specified port
            httpServer = new HTTPServer(metricsPort);
            LOGGER.info("Prometheus metrics server started on port {}", metricsPort);
        } catch (Exception e) {
            LOGGER.error("Error starting Prometheus HTTP server: {}", e.getMessage());
        }
    }

    public void startChangeStreamListener() {
        try {
            BsonDocument resumeToken = mediator.getLatestResumeToken();

            // Start the change stream with or without a resume token
            ChangeStreamIterable<Document> changeStream = resumeToken != null
                    ? changeStreamCollection.watch().resumeAfter(resumeToken)
                    : changeStreamCollection.watch();

            changeStream.forEach(changeStreamDocument -> {
                try {
                    // Get event's unique _id
                    int assignedThread = getThreadAssignment(
                            changeStreamDocument.getDocumentKey().get("_id").toString());
                    LOGGER.info("get one event and start handle.{}", changeStreamDocument);

                    // Submit the event to a specific thread based on the hash
                    executorService.submit(() -> {
                        mediator.processChangeEvent(changeStreamDocument, assignedThread);
                    });
                } catch (Exception e) {
                    LOGGER.error("Error processing changeStreamDocument: {}", changeStreamDocument, e);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Error starting change stream listener ", e);
        }
    }

    private int getThreadAssignment(String eventId) {
        // Hash the event ID and map it to a thread index (0 to threadCount-1)
        return Math.abs(eventId.hashCode()) % nums;
    }

    public void closeConnection() {
        try {
            if (httpServer != null) {
                httpServer.stop(); // Stop the HTTP server if it's running
                LOGGER.info("Prometheus metrics server stopped");
            }
        } catch (Exception e) {
            LOGGER.error("Error closing MongoDB connection: {}", e);
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
