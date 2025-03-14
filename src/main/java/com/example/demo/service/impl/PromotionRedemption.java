package com.example.demo.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class PromotionRedemption extends BusinessTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(Task1.class);

    private static final String TASK_COLLECTION_NAME = "tPromotionRedeemtion";
    private final MongoClient mongoClient;
    private final String databaseName;
    private final AwardCalculationService awardCalculationService;

    public PromotionRedemption(ResumeTokenService resumeTokenService, TpsCalculator tpsCalculator, MongoClient mongoClient,
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

        Document tPromotionRedemption = event.getFullDocument();
        if (tPromotionRedemption == null) return 0;

        // Find all matching tAwards using tPlayerID and tPrizeID
        List<Document> tAwardsList = tAwardsCollection.find(new Document("PlayerID", tPromotionRedemption.get("PlayerID"))
                .append("PrizeID", tPromotionRedemption.get("PrizeID"))).into(new ArrayList<>());

        if (tAwardsList.isEmpty()) return 0;

        int processedCount = 0;

        for (Document tAwards : tAwardsList) {
            // Get related documents using tAwards.tranId

            // Process award calculation
            Document memberAward = awardCalculationService.calculateAward(tAwards);
            if (memberAward == null) continue;
                // Upsert into member_awards collection
                memberAwardsCollection.updateOne(
                    new Document("TrainId", tAwards.get("TrainId")),  // Identify existing record
                    new Document("$set", memberAward),  // Update document fields
                    new com.mongodb.client.model.UpdateOptions().upsert(true) // Enable upsert
                ); 
            processedCount++;
        }

        LOGGER.info("process related Awards {}", processedCount);
        return 0;
    }
}
