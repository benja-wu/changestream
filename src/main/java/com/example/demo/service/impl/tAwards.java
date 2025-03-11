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
public class tAwards extends BusinessTask {
    
    private static final String TASK_COLLECTION_NAME = "tAwards";
    private final MongoClient mongoClient;
    private final String databaseName;
    private final AwardCalculationService awardCalculationService;

    public tAwards(ResumeTokenService resumeTokenService, TpsCalculator tpsCalculator, MongoClient mongoClient,
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
        MongoCollection<Document> memberAwards = database.getCollection("member_awards");

        Document tAwards = event.getFullDocument();
        if (tAwards == null) return 0;

        // Calculate the award data
        Document memberAward = awardCalculationService.calculateAward(tAwards);
        if (memberAward == null) return 0;

        // Store into member_awards collection
        memberAwards.insertOne(memberAward);
        return 1; 
    }
}
