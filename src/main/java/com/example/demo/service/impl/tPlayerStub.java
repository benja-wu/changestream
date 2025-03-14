package com.example.demo.service.impl;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.demo.metrics.PrometheusMetricsConfig;
import com.example.demo.metrics.TpsCalculator;
import com.example.demo.service.BusinessTask;
import com.example.demo.service.ResumeTokenService;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

@Service
public class tPlayerStub extends BusinessTask {

    private static final String TASK_COLLECTION_NAME = "tPlayerStub";
    private final MongoClient mongoClient;
    private final String databaseName;
    private final AwardCalculationService awardCalculationService;

    public tPlayerStub(ResumeTokenService resumeTokenService, TpsCalculator tpsCalculator, MongoClient mongoClient,
                       AwardCalculationService awardCalculationService, @Value("${spring.mongodb.database}") String databaseName) {
        super(resumeTokenService, tpsCalculator,
                PrometheusMetricsConfig.getInstance(TASK_COLLECTION_NAME),
                TASK_COLLECTION_NAME, mongoClient);
        this.mongoClient = mongoClient;
        this.awardCalculationService = awardCalculationService;
        this.databaseName = databaseName;
    }

    @Override
    public int processChange(ChangeStreamDocument<Document> event) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> tAwardsCollection = database.getCollection("tAwards");
        MongoCollection<Document> memberAwardsCollection = database.getCollection("member_awards");

        Document tPlayerStub = event.getFullDocument();
        if (tPlayerStub == null) return 0;

        // Retrieve corresponding tAwards using TrainId
        Document tAwards = tAwardsCollection.find(new Document("TrainId", tPlayerStub.get("TrainId"))).first();
        if (tAwards == null) return 0;

        // Process award calculation
        Document memberAward = awardCalculationService.calculateAward(tAwards);
        if (memberAward == null) return 0;

        // Upsert into member_awards collection
        memberAwardsCollection.updateOne(
            new Document("TrainId", tAwards.get("TrainId")),  // Identify existing record
            new Document("$set", memberAward),    // Update document fields
            new com.mongodb.client.model.UpdateOptions().upsert(true) // Enable upsert
        );
        return 0;
    }
}
