package com.example.demo.service.impl;

import java.util.Date;

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
public class hub_tPlayerPromo extends BusinessTask {

    private static final String TASK_COLLECTION_NAME = "hub_tPlayerPromo";
    private final MongoClient mongoClient;
    private final String databaseName;
    private final AwardCalculationService awardCalculationService;

    public hub_tPlayerPromo(ResumeTokenService resumeTokenService, TpsCalculator tpsCalculator, MongoClient mongoClient,
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

        Document tPlayerPromo = event.getFullDocument();
        if (tPlayerPromo == null) return 0;

        // Retrieve corresponding tAwards using TrainId
        Document tAwards = tAwardsCollection.find(new Document("TrainId", tPlayerPromo.get("TrainId"))).first();
        if (tAwards == null) return 0;
        Document query = new Document("tran_id", tAwards.get("TranId")).append("_ODS_schema", AwardCalculationService.ODS_SCHEMA_VERSION);

        // Calculate the award data
        Document memberAward = awardCalculationService.calculateAward(tAwards);
        if (memberAward == null) return 0;

        // Upsert into member_awards collection
        memberAwardsCollection.updateOne(
            query,
            new Document("$set", memberAward).append("$setOnInsert", new Document("_ODS_created_dtm", new Date())),  // Update document fields
            new com.mongodb.client.model.UpdateOptions().upsert(true) // Enable upsert
        );
        return 0; 
    }
}
