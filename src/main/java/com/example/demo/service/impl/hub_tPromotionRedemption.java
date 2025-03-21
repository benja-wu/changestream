package com.example.demo.service.impl;

import java.util.ArrayList;
import java.util.Date;
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
public class hub_tPromotionRedemption extends BusinessTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(Task1.class);

    private static final String TASK_COLLECTION_NAME = "hub_tPromotionRedeemtion";
    private final MongoClient mongoClient;
    private final String databaseName;
    private final AwardCalculationService awardCalculationService;

    public hub_tPromotionRedemption(ResumeTokenService resumeTokenService, TpsCalculator tpsCalculator, MongoClient mongoClient,
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

        // Find all matching tAwards using PlayerID and PrizeID
        List<Document> tAwardsList = tAwardsCollection.find(new Document("PlayerID", tPromotionRedemption.get("PlayerID"))
                .append("PrizeID", tPromotionRedemption.get("PrizeID"))).into(new ArrayList<>());

        if (tAwardsList.isEmpty()) return 0;

        int processedCount = 0;

        for (Document tAwards : tAwardsList) {
            // Define unique identifier: TranID + _ODS_schema
            Document query = new Document("tran_id", tAwards.get("TranId"))
                            .append("_ODS_schema", AwardCalculationService.ODS_SCHEMA_VERSION);


            // Process award calculation
            Document memberAward = awardCalculationService.calculateAward(tAwards);
            if (memberAward == null) continue;

            // Upsert into member_awards collection
            memberAwardsCollection.updateOne(
                query,
                new Document("$set", memberAward).append("$setOnInsert", new Document("_ODS_created_dtm", new Date())),  // Update document fields
                new com.mongodb.client.model.UpdateOptions().upsert(true) // Enable upsert
            );

            processedCount++;
        }

        LOGGER.info("Processed {} related Awards", processedCount);
        return processedCount;
    }

}
