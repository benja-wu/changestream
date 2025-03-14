package com.example.demo.service;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoCollection;

import jakarta.annotation.PostConstruct;

@Service
@DependsOn("collectionMap")
public class EventProcessingMediator {
    private static final int MAX_RETRIES = 5;
    private static final long RETRY_DELAY_MS = 1000; // 1 second
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessingMediator.class);
    
    // Maps collection names to their respective BusinessTask implementations
    private final Map<String, BusinessTask> tasks = new HashMap<>();
    
    // Stores the MongoDB collections mapped by their names
    private final Map<String, MongoCollection<Document>> collectionMap;
    
    // Stores the executor service for each collection to manage parallel processing
    private final Map<String, ExecutorService> executorServicesMap = new HashMap<>();

    @Value("${spring.threadpool.nums}")
    private int nums; // Number of threads in the thread pool

    @Value("${spring.lifecycle.timeout-per-shutdown-phase:30s}")
    private String shutdownTimeoutString;
    private long shutdownTimeout;

    @Autowired
    public EventProcessingMediator(Map<String, MongoCollection<Document>> collectionMap, List<BusinessTask> businessTasks) {
        this.collectionMap = collectionMap;
        LOGGER.info("Initializing with collectionMap: {}", collectionMap);
        
        // Associate each business task with its respective collection
        for (BusinessTask task : businessTasks) {
            String collectionName = task.getCollectionName();
            MongoCollection<Document> collection = collectionMap.get(collectionName);
            if (collection != null) {
                task.setCollection(collection);
                tasks.put(collectionName, task);
                LOGGER.info("Mapped task {} to collection {}", task.getClass().getSimpleName(), collectionName);
            } else {
                LOGGER.warn("No collection found for task: {}", collectionName);
            }
        }
    }

    @PostConstruct
    public void init() {
        // Parse shutdown timeout duration from configuration
        this.shutdownTimeout = Duration.parse("PT" + shutdownTimeoutString.replaceAll("[^0-9]", "") + "S").getSeconds();
        
        // Initialize a fixed thread pool for each collection
        for (String collectionName : tasks.keySet()) {
            AtomicInteger threadCounter = new AtomicInteger(0); 
            ThreadFactory threadFactory = r -> {

                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("ChangeStream-" + collectionName+"-T"+ threadCounter.getAndIncrement());
                return t;
            };
            ExecutorService executorService = Executors.newFixedThreadPool(nums, threadFactory);
            executorServicesMap.put(collectionName, executorService);
            LOGGER.info("Created a fixed-thread pool of size {} for collection {}", nums, collectionName);
        }
    }

    // Starts change stream listeners for all collections
    public void startChangeStreamListeners() {
        for (String collectionName : tasks.keySet()) {
            LOGGER.info("Starting listener for collection: {}", collectionName);
            new Thread(() -> listenForChanges(collectionName), "Listener-" + collectionName).start();
        }
        LOGGER.info("Started all change stream listeners.");
    }

    /**
     * Listens for changes on a specific collection and submits tasks for processing
     * with max retry attempts. 
     * */ 
    private void listenForChanges(String collectionName) {
        int retryCount = 0;
    
        // Continue attempting to listen until max retries are reached
        while (retryCount < MAX_RETRIES) {
            BusinessTask task = tasks.get(collectionName);
            if (task == null) {
                LOGGER.warn("No task for collection: {}", collectionName);
                return;
            }
    
            // Retrieve the resume token for restarting the change stream
            BsonDocument resumeToken = task.resumeTokenService != null ?
                task.resumeTokenService.getResumeToken(collectionName) : null;
            LOGGER.info("Starting change stream for {} with resume token: {}", collectionName, resumeToken);
    
            try {
                // Start the change stream and process events
                task.changeStreamIterator(resumeToken).forEach(event -> {
                    // Handle invalidation event
                    if ("invalidate".equals(event.getOperationType())) {
                        LOGGER.info("Change stream invalidated for {}. Will attempt to restart.", collectionName);
                        throw new RuntimeException("Invalidated"); // Break the iterator to trigger a retry
                    }
    
                    // Validate the event's fullDocument
                    Document fullDocument = event.getFullDocument();
                    if (fullDocument == null || !fullDocument.containsKey("_id")) {
                        LOGGER.error("Event missing _id in fullDocument: {}", event);
                        return; // Skip this event
                    }
    
                    // Submit event processing to the executor service
                    ExecutorService executorService = executorServicesMap.get(collectionName);
                    executorService.submit(() -> {
                        try {
                            LOGGER.info("🔄 Processing event on thread: {} for collection: {}", 
                                Thread.currentThread().getName(), collectionName);
                            task.startProcessing(Thread.currentThread().getName(), event);
                        } catch (Exception e) {
                            LOGGER.error("Failed to process event {} {}", event, e);
                        }
                    });
                });
    
                // If the forEach loop exits normally, it’s unexpected for a change stream
                LOGGER.warn("Change stream closed normally for {}. Retrying...", collectionName);
                retryCount++;
    
            } catch (Exception e) {
                // Handle specific invalidation case
                if (e.getMessage().equals("Invalidated")) {
                    LOGGER.info("Change stream invalidated. Retrying...");
                } else {
                    // Log other unexpected errors
                    LOGGER.error("Error in change stream for {}: {}", collectionName, e.getMessage(), e);
                }
    
                // Increment retry count and add delay before next attempt
                retryCount++;
                if (retryCount < MAX_RETRIES) {
                    try {
                        Thread.sleep(RETRY_DELAY_MS); // Delay to avoid rapid retries
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOGGER.error("Interrupted during retry delay", ie);
                        return;
                    }
                }
            }
        }
    
        // Log failure and stop if max retries are exhausted
        LOGGER.error("Max retries reached for collection {}. Stopping listener.", collectionName);
        // Optional: Add further action here, e.g., notify an admin or trigger a shutdown
    }

    // Gracefully shuts down the executor services for all collections
    public void shutdown() {
        LOGGER.info("Shutting down...");
        for (ExecutorService executor : executorServicesMap.values()) {
            executor.shutdown();
        }
        try {
            for (ExecutorService executor : executorServicesMap.values()) {
                if (!executor.awaitTermination(shutdownTimeout, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    LOGGER.warn("Forced shutdown of executor after timeout");
                }
            }
        } catch (InterruptedException e) {
            for (ExecutorService executor : executorServicesMap.values()) {
                executor.shutdownNow();
            }
            Thread.currentThread().interrupt();
            LOGGER.error("Shutdown interrupted", e);
        }
        LOGGER.info("Shutdown complete.");
    }
}
