package com.example.demo.service.impl;

import java.util.List;

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
public class tPlayerPoints extends BusinessTask {

    private static final String TASK_COLLECTION_NAME = "tPlayerPoints";
    private final MongoClient mongoClient;
    private final String databaseName;
    private final AwardCalculationService awardCalculationService;

    public tPlayerPoints(ResumeTokenService resumeTokenService, TpsCalculator tpsCalculator, MongoClient mongoClient,
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

        Document tPlayerPoints = event.getFullDocument();
        if (tPlayerPoints == null) return 0;

        // Retrieve all corresponding tAwards using TranId
        List<Document> tAwardsList = tAwardsCollection.find(new Document("TranId", tPlayerPoints.get("TranId"))).into(new java.util.ArrayList<>());
        if (tAwardsList.isEmpty()) return 0;

        // Process each related tAwards document
        for (Document tAwards : tAwardsList) {
            Document memberAward = awardCalculationService.calculateAward(tAwards);
            if (memberAward != null) {
                memberAwardsCollection.insertOne(memberAward);
            }
        }

        return 1;
    }
}
