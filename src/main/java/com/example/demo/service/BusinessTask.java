package com.example.demo.service;

import org.bson.BsonDocument;
import org.bson.Document;

import com.example.demo.metrics.PrometheusMetricsConfig;
import com.example.demo.metrics.TpsCalculator;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

/**
 * BusinessTask encapsulates the logic for handling change stream events, including resuming from a specific token 
 * and processing each event.
 */
public abstract class BusinessTask {
    protected final ResumeTokenService resumeTokenService;
    protected final TpsCalculator tpsCalculator;
    protected final PrometheusMetricsConfig metricsConfig;
    protected final String collectionName;
    protected MongoCollection<Document> collection;
    protected final MongoClient mongoClient;

    public BusinessTask(ResumeTokenService resumeTokenService, TpsCalculator tpsCalculator,
                        PrometheusMetricsConfig metricsConfig, String collectionName, MongoClient mongoClient) {
        this.resumeTokenService = resumeTokenService;
        this.tpsCalculator = tpsCalculator;
        this.metricsConfig = metricsConfig;
        this.collectionName = collectionName;
        this.mongoClient = mongoClient;
    }

    public void setCollection(MongoCollection<Document> collection) {
        this.collection = collection;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void startProcessing(String threadName, ChangeStreamDocument<Document> event) {
        long startTimeMillis = System.currentTimeMillis();
        long eventMillis = event.getClusterTime().getTime() * 1000;

        tpsCalculator.recordEvent(collectionName, threadName);

        int ret = processChange(event);

        if (ret == 0) {
            metricsConfig.incrementTotalEventsHandledSuccessfully();
        }

        double eventLag = startTimeMillis - eventMillis;
        double tps = tpsCalculator.calculateTps(collectionName, threadName);
        long durationMillis = System.currentTimeMillis() - startTimeMillis;
        double durationSeconds = durationMillis / 1000.0;

        metricsConfig.getEventLagPerThread().labels(threadName).set(eventLag);
        metricsConfig.incrementTotalEventsHandled();
        metricsConfig.getTpsPerThread().labels(threadName).set(tps);
        metricsConfig.getEventProcessDuration().observe(durationSeconds);
        metricsConfig.getP99ProcessingTime().observe(durationMillis);
        if (event.getResumeToken() != null) {
            resumeTokenService.saveResumeToken(event.getClusterTime(), event.getResumeToken(), threadName, collectionName);
        }
    }

    protected abstract int processChange(ChangeStreamDocument<Document> event);


    public ChangeStreamIterable<Document> changeStreamIterator(BsonDocument resumeToken) {
        if (collection == null) {
            throw new IllegalStateException("Collection has not been set for " + collectionName);
        }

        ChangeStreamIterable<Document> changeStream = resumeToken != null
                ? collection.watch().resumeAfter(resumeToken)
                : collection.watch();
        changeStream.fullDocument(FullDocument.UPDATE_LOOKUP);
        changeStream.fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE);

        System.out.printf("✅ Change stream open for collection : %s  \n", collectionName);
        return changeStream;
    }
}
