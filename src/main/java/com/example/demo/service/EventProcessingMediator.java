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

    // Listens for changes on a specific collection and submits tasks for processing
    private void listenForChanges(String collectionName) {
        BusinessTask task = tasks.get(collectionName);
        if (task == null) {
            LOGGER.warn("No task for collection: {}", collectionName);
            return;
        }

        // Retrieve the resume token to resume from the last known position in the change stream
        BsonDocument resumeToken = task.resumeTokenService != null ?
            task.resumeTokenService.getResumeToken(collectionName) : null;
        LOGGER.info("Starting change stream for {} with resume token: {}", collectionName, resumeToken);

        // Process each event from the change stream
        task.changeStreamIterator(resumeToken).forEach(event -> {
            Document fullDocument = event.getFullDocument();
            if (fullDocument == null || !fullDocument.containsKey("_id")) {
                LOGGER.error("Ensure using MongoDB 6.0 above, event missing _id in fullDocument: {}", event);
                return;
            }

            // Submit the event to the executor for processing asynchronously
            ExecutorService executorService = executorServicesMap.get(collectionName);
            executorService.submit(() -> {
                try {
                    LOGGER.info("🔄 Processing event on thread: {} for collection: {}", 
                    Thread.currentThread().getName(), collectionName);
                    // Introduce a sleep to simulate processing time (e.g., 3 seconds)
                    // Thread.sleep(3000);
                    task.startProcessing(Thread.currentThread().getName(), event);
                } catch (Exception e) {
                    LOGGER.error("Failed to process event {} {}", event, e);
                }
            });
            LOGGER.info("Submitted event to thread pool for collection: {}", collectionName);
        });
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
