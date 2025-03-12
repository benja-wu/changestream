package com.example.demo.service;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoCollection;

import jakarta.annotation.PostConstruct;

/**
 * EventProcessingMediator coordinates multiple BusinessTask instances, each with a set of single-threaded executors
 * for processing change stream events from MongoDB collections based on event ID mapping.
 */
@Service
@DependsOn("collectionMap")
public class EventProcessingMediator {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessingMediator.class);
    private final Map<String, BusinessTask> tasks = new HashMap<>();
    private final Map<String, MongoCollection<Document>> collectionMap;
    private final Map<String, ExecutorService[]> executorServicesMap = new HashMap<>();

    @Value("${spring.threadpool.nums}")
    private int nums; // Number of threads (threadpool_size)

    @Value("${spring.lifecycle.timeout-per-shutdown-phase:30s}")
    private String shutdownTimeoutString;
    private long shutdownTimeout;

    @Autowired
    public EventProcessingMediator(Map<String, MongoCollection<Document>> collectionMap, List<BusinessTask> businessTasks) {
        this.collectionMap = collectionMap;
        LOGGER.info("Initializing with collectionMap: {}", collectionMap);
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
        this.shutdownTimeout = Duration.parse("PT" + shutdownTimeoutString.replaceAll("[^0-9]", "") + "S").getSeconds();
        for (String collectionName : tasks.keySet()) {
            ExecutorService[] executors = new ExecutorService[nums];
            for (int i = 0; i < nums; i++) {
                final int index = i;
                ThreadFactory threadFactory = r -> {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    t.setName("ChangeStream-" + collectionName + "-Thread-" + index);
                    return t;
                };
                executors[i] = Executors.newSingleThreadExecutor(threadFactory);
            }
            executorServicesMap.put(collectionName, executors);
            LOGGER.info("Created {} single-threaded executors for collection {}", nums, collectionName);
        }
    }

    public void startChangeStreamListeners() {
        for (String collectionName : tasks.keySet()) {
            LOGGER.info("Starting listener for collection: {}", collectionName);
            new Thread(() -> listenForChanges(collectionName), "Listener-" + collectionName).start();
        }
        LOGGER.info("Started all change stream listeners.");
    }

    private void listenForChanges(String collectionName) {
        BusinessTask task = tasks.get(collectionName);
        if (task == null) {
            LOGGER.warn("No task for collection: {}", collectionName);
            return;
        }

        BsonDocument resumeToken = task.resumeTokenService != null ?
            task.resumeTokenService.getResumeToken(collectionName) : null;
        LOGGER.info("Starting change stream for {} with resume token: {}", collectionName, resumeToken);

        task.changeStreamIterator(resumeToken).forEach(event -> {
            Document fullDocument = event.getFullDocument();
            if (fullDocument == null || !fullDocument.containsKey("_id")) {
                LOGGER.error("Event missing _id in fullDocument: {}", event);
                return;
            }

            // Extract _id from fullDocument (it's an ObjectId)
            Object idObject = fullDocument.get("_id");
            String idString = (idObject instanceof ObjectId) ? ((ObjectId) idObject).toHexString() : idObject.toString();

            // Compute thread index using hash code of _id
            int hashCode = idString.hashCode();
            int threadIndex = Math.abs(hashCode % nums); // Ensure non-negative index within [0, nums-1]

            // Submit to the corresponding single-threaded executor
            ExecutorService executor = executorServicesMap.get(collectionName)[threadIndex];
            executor.submit(() -> {
                try {
                    task.startProcessing(Thread.currentThread().getName(), event);
                } catch (Exception e) {
                    LOGGER.error("Failed to process event {} {}",  event,e );
                }
            });
            LOGGER.info("Submitted event with _id {} to thread index {}", idString, threadIndex);
        });
    } 

    public void shutdown() {
        LOGGER.info("Shutting down...");
        for (ExecutorService[] executors : executorServicesMap.values()) {
            for (ExecutorService executor : executors) {
                executor.shutdown();
            }
        }
        try {
            for (ExecutorService[] executors : executorServicesMap.values()) {
                for (ExecutorService executor : executors) {
                    if (!executor.awaitTermination(shutdownTimeout, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                        LOGGER.warn("Forced shutdown of executor after timeout");
                    }
                }
            }
        } catch (InterruptedException e) {
            for (ExecutorService[] executors : executorServicesMap.values()) {
                for (ExecutorService executor : executors) {
                    executor.shutdownNow();
                }
            }
            Thread.currentThread().interrupt();
            LOGGER.error("Shutdown interrupted", e);
        }
        LOGGER.info("Shutdown complete.");
    }
}