package com.example.demo.service;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.support.RetrySynchronizationManager;
import org.springframework.stereotype.Service;
import org.springframework.context.ApplicationContext;
import org.springframework.retry.RetryContext;

import com.mongodb.client.MongoCollection;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
@DependsOn("collectionMap")
public class EventProcessingMediator {
    private static final int MAX_RETRIES = 5;
    private static final long RETRY_DELAY_MS = 1000; // 1 second
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessingMediator.class);
    
    // Maps collection names to their respective BusinessTask implementations
    private final Map<String, BusinessTask> tasks = new HashMap<>();
    
    @Autowired
    private ApplicationContext applicationContext;

    // Stores the MongoDB collections mapped by their names
    private final Map<String, MongoCollection<Document>> collectionMap;

    // Stores the executor service for each collection to manage parallel processing
    private final Map<String, ExecutorService> executorServicesMap = new HashMap<>();

    // The default queue capacity for each thread pool, assume average 14KB per task, 3500 ~ 50MB for heap
    @Value("${spring.threadpool.queue-capacity:3500}")
    private int queueCapacity;

    @Value("${spring.threadpool.nums}")
    private int nums; // Number of threads in the thread pool

    @Value("${spring.lifecycle.timeout-per-shutdown-phase:30s}")
    private String shutdownTimeoutString;
    private long shutdownTimeout;

    @Autowired
    public EventProcessingMediator(Map<String, MongoCollection<Document>> collectionMap, List<BusinessTask> businessTasks) {
        this.collectionMap = collectionMap;
        LOGGER.info("Initializing with collectionMap: {}", this.collectionMap);
        
        // Associate each business task with its respective collection
        for (BusinessTask task : businessTasks) {
            String collectionName = task.getCollectionName();
            MongoCollection<Document> collection = this.collectionMap.get(collectionName);
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

            // 1. Define the bounded queue
            BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(queueCapacity);

            // 2. Define the back-pressure policy
            // This forces the LISTENER thread to run the task if the queue is full,
            // which pauses the listener and stops it from flooding the queue.
            ThreadPoolExecutor.CallerRunsPolicy rejectionPolicy = new ThreadPoolExecutor.CallerRunsPolicy();

            // 3. Create the new, safe executor
            ExecutorService executorService = new ThreadPoolExecutor(
                nums,                 // corePoolSize
                nums,                 // maximumPoolSize (same as core for a "fixed" pool)
                0L,     // keepAliveTime
                TimeUnit.MILLISECONDS,
                workQueue,            // The new bounded queue
                threadFactory,        // Your existing threadFactory
                rejectionPolicy       // The new back-pressure policy
            );

            executorServicesMap.put(collectionName, executorService);
            LOGGER.info("Created a SAFE, fixed-thread pool of size {} with queue capacity {} for collection {}",
                nums, queueCapacity, collectionName);
        }
    }

    // Starts change stream listeners for all collections
    public void startChangeStreamListeners() {
         // Get the proxied bean so @Retryable is actually applied
        EventProcessingMediator proxy = applicationContext.getBean(EventProcessingMediator.class);

        for (String collectionName : tasks.keySet()) {
            LOGGER.info("Starting listener for collection: {}", collectionName);
            new Thread(() -> proxy.listenForChanges(collectionName), "Listener-" + collectionName).start();
        }
        LOGGER.info("Started all change stream listeners.");
    }

    /**
     * Listens for changes on a specific collection and submits tasks for processing
     * with max retry attempts, also with 1-second backoff delay.
     * */ 
    @Retryable(
        value = { Exception.class }, // Retry on any exception
        maxAttempts = MAX_RETRIES,
        backoff = @Backoff(delay = RETRY_DELAY_MS)
    )
    public void listenForChanges(String collectionName) {
        // Log the current attempt number (optional but helpful)
        RetryContext retryContext = RetrySynchronizationManager.getContext();
        int retryCount = (retryContext != null) ? retryContext.getRetryCount() : 0;
        if (retryCount > 0) {
            LOGGER.warn("Retrying change stream for {}. Attempt {}/{}", collectionName, retryCount + 1, MAX_RETRIES);
        }

        // Continue attempting to listen until max retries are reached
        BusinessTask task = tasks.get(collectionName);
        if (task == null) {
            LOGGER.warn("No task for collection: {}", collectionName);
            return;
        }

        // Retrieve the resume token for restarting the change stream
        BsonDocument resumeToken = task.resumeTokenService != null ?
            task.resumeTokenService.getResumeToken(collectionName) : null;
        LOGGER.info("Starting change stream for {} with resume token: {}", collectionName, resumeToken);

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

        // If the stream closes normally, it's an error state
        LOGGER.warn("Change stream closed normally for {}. Retrying...", collectionName);
        throw new RuntimeException("Change stream closed unexpectedly");
    }

    @PreDestroy // This annotation makes Spring call shutdown() automatically
    public void shutdown() {
        LOGGER.info("Shutting down executor services...");
        for (Map.Entry<String, ExecutorService> entry : executorServicesMap.entrySet()) {
            LOGGER.info("Shutting down pool for {}", entry.getKey());
            ExecutorService executor = entry.getValue();
            executor.shutdown();
        }
        try {
            for (ExecutorService executor : executorServicesMap.values()) {
                if (!executor.awaitTermination(shutdownTimeout, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    LOGGER.warn("Forced shutdown of an executor after timeout");
                }
            }
        } catch (InterruptedException e) {
            for (ExecutorService executor : executorServicesMap.values()) {
                executor.shutdownNow();
            }
            Thread.currentThread().interrupt();
            LOGGER.error("Shutdown interrupted", e);
        }
        LOGGER.info("All executor services shutdown complete.");
    }
}
