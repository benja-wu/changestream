package com.example.demo.service.impl;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

@Service
public class AwardCalculationService {

    private final MongoClient mongoClient;
    private final String databaseName;

    public AwardCalculationService(MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
    }

    public Document calculateAward(Document tAwards) {
        if (tAwards == null) return null;
        MongoDatabase database = mongoClient.getDatabase(databaseName);

        MongoCollection<Document> tPlayerStub = database.getCollection("tPlayerStub");
        MongoCollection<Document> tPlayerPromo = database.getCollection("tPlayerPromo1");
        MongoCollection<Document> tPlayerPoints = database.getCollection("tPlayerPoints");
        MongoCollection<Document> tPrize = database.getCollection("tPrize");
        MongoCollection<Document> tPrizeLocnMapping = database.getCollection("tPrizeLocnMapping");
        MongoCollection<Document> memberProfileCollection = database.getCollection("member_profile");
        //MongoCollection<Document> tTranCode = database.getCollection("tTranCode");
        MongoCollection<Document> tHUBPromotionRuleOutCome = database.getCollection("tHUBPromotionRuleOutCome");

        // Convert tAwards fields to snake_case
        Document memberAward = convertToSnakeCase(tAwards);

        // Populate tPlayerStub
        Document playerStub = tPlayerStub.find(new Document("TrainId", tAwards.get("TrainId"))).first();
        memberAward.put("player_stub", convertToSnakeCase(filterFields(playerStub, Set.of("CreatedDtm", "CreatedBy", "ModifiedDtm", "DataRowVersion"))));

        // Populate tPlayerPromo
        Document playerPromo = tPlayerPromo.find(new Document("TrainId", tAwards.get("TrainId"))).first();
        memberAward.put("player_promo1", convertToSnakeCase(filterFields(playerPromo, Set.of("CreatedDtm", "CreatedBy", "ModifiedDtm", "DataRowVersion"))));

        // Populate tPlayerPoints
        List<Document> playerPoints = tPlayerPoints.find(new Document("TranId", tAwards.get("TranId"))).into(new java.util.ArrayList<>());
        List<Document> filteredPlayerPoints = playerPoints.stream()
                .map(doc -> convertToSnakeCase(filterFields(doc, Set.of("CreatedDtm", "CreatedBy", "ModifiedDtm", "DataRowVersion"))))
                .collect(Collectors.toList());
        memberAward.put("player_points", filteredPlayerPoints);

        // Populate tPrize (only required fields)
        Document prize = tPrize.find(new Document("PrizeId", tAwards.get("PrizeId"))).first();
        Document filteredPrize = convertToSnakeCase(extractFields(prize, Set.of("PrizeId", "PrizeCode", "PrizeName", "AwardCode")));
        memberAward.put("prize", filteredPrize);

        // Populate tPrizeLocnMapping (only required fields)
        Document prizeLocnMapping = tPrizeLocnMapping.find(new Document("PrizeId", tAwards.get("PrizeId"))).first();
        memberAward.put("prize_locn_mapping", convertToSnakeCase(extractFields(prizeLocnMapping, Set.of("CasinoId", "LocnId", "LocnCode"))));

        // Copy prize_code and prize_name into prize subdocument
        if (filteredPrize != null) {
            memberAward.put("prize_code", filteredPrize.get("prize_code"));
            memberAward.put("prize_name", filteredPrize.get("prize_name"));
        }

        // Optimized PrizeType Calculation, without using tTranCode collection
        int prizeType = -1; // Default value

        // Get TranCodeID directly from tAwards
        int tranCodeID = tAwards.getInteger("TranCodeID", -1);

        // Retrieve targetRID from tHUBPromotionRuleOutCome where PlayerID = tAwards.PlayerID
        Document hub = tHUBPromotionRuleOutCome.find(new Document("PlayerID", tAwards.get("PlayerID"))).first();
        if (hub != null) {
            Object targetRID = hub.get("RID");

            // Find all PIDs where RID = targetRID
            List<Document> pidArray = tHUBPromotionRuleOutCome.find(new Document("RID", targetRID))
                    .into(new java.util.ArrayList<>());
            List<Object> pidValues = pidArray.stream()
                    .map(doc -> doc.get("PID"))  // Extracting only PIDs
                    .collect(Collectors.toList());

            //  Apply PrizeType conditions
            if (tranCodeID == 10 || tranCodeID == 11 || tranCodeID == 12) {
                prizeType = 2;
            } else if (tranCodeID == 4) {
                prizeType = 3;
            } else if (tAwards.containsKey("RID") && pidValues.contains(tAwards.get("RID"))) {
                prizeType = 4;
            }
        }

        // Store prize_type in member_awards
        memberAward.put("prize_type", prizeType);
        // Store values in member_awards
        boolean isDocPmprize = "P".equals(tAwards.getString("Doc"));
        memberAward.put("is_doc_pmprize", isDocPmprize);


        // Retrieve member_profile where tAwards.player_id = member_profile.player_id
        Document memberProfile = memberProfileCollection.find(new Document("player_id", tAwards.get("PlayerID"))).first();

        // Extract member_no if member_profile exists
        if (memberProfile != null) {
            memberAward.put("member_no", memberProfile.getString("member_no"));

            // Extract required fields for member_profile object
            Document filteredMemberProfile = extractFields(memberProfile, Set.of(
                "is_active_program",
                "club_state",
                "club_state_name",
                "primary_host_id",
                "secondary_host_id",
                "primary_host_num",
                "secondary_host_num",
                "is_banned",
                "is_inactive"
            ));
            
            // Convert to snake_case and store in member_awards
            memberAward.put("member_profile", convertToSnakeCase(filteredMemberProfile));
        }

        return memberAward;
    }

    /**
     * Filters out unwanted fields from a document.
     */
    private Document filterFields(Document document, Set<String> excludeFields) {
        if (document == null) return null;
        Document filteredDoc = new Document();
        for (String key : document.keySet()) {
            if (!excludeFields.contains(key)) {
                filteredDoc.put(key, document.get(key));
            }
        }
        return filteredDoc;
    }

    /**
     * Extracts only the required fields from a document.
     */
    private Document extractFields(Document document, Set<String> includeFields) {
        if (document == null) return null;
        Document extractedDoc = new Document();
        for (String key : includeFields) {
            if (document.containsKey(key)) {
                extractedDoc.put(key, document.get(key));
            }
        }
        return extractedDoc;
    }

    /**
     * Converts all field names from CamelCase to snake_case.
     */
    private Document convertToSnakeCase(Document document) {
        if (document == null) return null;
        Document snakeCaseDoc = new Document();
        for (String key : document.keySet()) {
            snakeCaseDoc.put(toSnakeCase(key), document.get(key));
        }
        return snakeCaseDoc;
    }

    /**
     * Converts CamelCase to snake_case, ensuring proper handling of acronyms and multi-word fields.
     */
    private String toSnakeCase(String camelCase) {
        return camelCase
                .replaceAll("([a-z0-9])([A-Z])", "$1_$2")  // Standard CamelCase to snake_case conversion
                .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2") // Ensures handling of consecutive uppercase words (acronyms)
                .toLowerCase();
    }
}