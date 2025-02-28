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

/**
 * EventProcessingMediator coordinates multiple BusinessTask instances, each responsible for 
 * a different MongoDB collection, and manages their lifecycle
 */
@Service
@DependsOn("collectionMap")
public class EventProcessingMediator {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessingMediator.class);
    private final Map<String, BusinessTask> tasks = new HashMap<>();
    private final Map<String, MongoCollection<Document>> collectionMap;
    private ExecutorService executorService;
    private final AtomicInteger threadCounter = new AtomicInteger();

    @Value("${spring.threadpool.nums}")
    private int nums;

    @Value("${spring.lifecycle.timeout-per-shutdown-phase:30s}")
    private String shutdownTimeoutString;
    private long shutdownTimeout;

    @Autowired
    public EventProcessingMediator(Map<String, MongoCollection<Document>> collectionMap, List<BusinessTask> businessTasks) {
        this.collectionMap = collectionMap;
        
        LOGGER.info("the collectionMap is {}", collectionMap);
        for (BusinessTask task : businessTasks) {
            String collectionName = task.getCollectionName();
            MongoCollection<Document> collection = collectionMap.get(collectionName);
            
            if (collection != null) {
                task.setCollection(collection);
                tasks.put(collectionName, task);
                LOGGER.info("Mapped task {} to collection {}", task.getClass().getSimpleName(), collectionName);
            } else {
                LOGGER.warn("No matching collection found for task: {}", collectionName);
            }
        }
    }

    @PostConstruct
    public void init() {
        this.shutdownTimeout = Duration.parse("PT" + shutdownTimeoutString.replaceAll("[^0-9]", "") + "S")
                .getSeconds();
        executorService = Executors.newFixedThreadPool(nums, daemonThreadFactory());
    }

    private ThreadFactory daemonThreadFactory() {
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            thread.setName("ChangeStream-" + threadCounter.getAndIncrement());
            return thread;
        };
    }

    public void startChangeStreamListeners() {
        for (String collectionName : tasks.keySet()) {
            LOGGER.info("task is for collection : {} ",collectionName ); 
            executorService.submit(() -> listenForChanges(collectionName));
        }
        LOGGER.info("Started change stream listeners for all collections.");
    }

    private void listenForChanges(String collectionName) {
        BusinessTask task = tasks.get(collectionName);
        if (task == null) {
            LOGGER.warn("No task found for collection: {}", collectionName);
            return;
        }
    
        if (task.resumeTokenService == null) {
            LOGGER.error("❌ resumeTokenService is NULL for collection: {}", collectionName);
        } else {
            LOGGER.info("✅ resumeTokenService is initialized for collection: {}", collectionName);
        }
    
        LOGGER.info("🔍 Retrieving resume token for collection: {}", collectionName);
        BsonDocument resumeToken = task.resumeTokenService.getResumeToken(collectionName);
    
        if (resumeToken == null) {
            LOGGER.warn("⚠ No resume token found for {}, starting from the latest event", collectionName);
        } else {
            LOGGER.info("✅ Using resume token for {}: {}", collectionName, resumeToken);
        }
    
        LOGGER.info("Starting change stream for collection: {}", collectionName);
    
        task.changeStreamIterator(resumeToken).forEach(event -> {
            LOGGER.info("🟢 Received change event for collection: {}", collectionName);
            String threadName = Thread.currentThread().getName();
            task.startProcessing(threadName, event);
        });
    }
    

    public void shutdown() {
        LOGGER.info("Shutdown requested, closing change stream...");
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(shutdownTimeout, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
