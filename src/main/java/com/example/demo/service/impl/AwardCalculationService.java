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
    private static final Set<String> EXCLUDED_FIELDS = Set.of("_id", "CreatedDtm", "CreatedBy", "ModifiedDtm", "DataRowVersion");

    public AwardCalculationService(MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
    }

    /**
     * Calculate member_award data based on tAwards,tPlayerStub,tPlayerPromo1,tPlayerPoints,tPrize,tPrizeLocnMapping,
     * member_profile,tHUBPromotionRuleOutCome 
     */
    public Document calculateAward(Document tAwards) {
        if (tAwards == null) return null;
        MongoDatabase database = mongoClient.getDatabase(databaseName);

        MongoCollection<Document> tPlayerStub = database.getCollection("tPlayerStub");
        MongoCollection<Document> tPlayerPromo = database.getCollection("tPlayerPromo1");
        MongoCollection<Document> tPlayerPoints = database.getCollection("tPlayerPoints");
        MongoCollection<Document> tPrize = database.getCollection("tPrize");
        MongoCollection<Document> tPrizeLocnMapping = database.getCollection("tPrizeLocnMapping");
        MongoCollection<Document> memberProfileCollection = database.getCollection("member_profile");
        MongoCollection<Document> tHUBPromotionRuleOutCome = database.getCollection("tHUBPromotionRuleOutCome");

        // Convert tAwards fields to snake_case while filtering out unwanted fields
        Document memberAward = convertToSnakeCase(filterFields(tAwards, Set.of("_id")));

        // Populate related collections
        memberAward.put("player_stub", getFilteredAndConvertedDocument(tPlayerStub, "TrainId", tAwards.get("TrainId")));
        memberAward.put("player_promo1", getFilteredAndConvertedDocument(tPlayerPromo, "TrainId", tAwards.get("TrainId")));

        // Populate tPlayerPoints
        List<Document> playerPoints = tPlayerPoints.find(new Document("TranId", tAwards.get("TranId"))).into(new java.util.ArrayList<>());
        List<Document> filteredPlayerPoints = playerPoints.stream()
                .map(doc -> convertToSnakeCase(filterFields(doc, EXCLUDED_FIELDS)))
                .collect(Collectors.toList());
        memberAward.put("player_points", filteredPlayerPoints);

        // Populate tPrize
        Document prize = getFilteredAndConvertedDocument(tPrize, "PrizeId", tAwards.get("PrizeId"), Set.of("PrizeId", "PrizeCode", "PrizeName", "AwardCode"));
        memberAward.put("prize", prize);

        // Populate tPrizeLocnMapping
        memberAward.put("prize_locn_mapping", getFilteredAndConvertedDocument(tPrizeLocnMapping, "PrizeId", tAwards.get("PrizeId"), Set.of("CasinoId", "LocnId", "LocnCode")));


        // PrizeType Calculation
        int prizeType = calculatePrizeType(tAwards, tHUBPromotionRuleOutCome);
        memberAward.put("award_prize_type", prizeType); 

        // Store values in member_awards
        boolean isDocPmprize = "P".equals(tAwards.getString("Doc"));
        memberAward.put("is_doc_pmprize", isDocPmprize);
        // Retrieve member_profile
        memberAward.putAll(getMemberProfile(memberProfileCollection, tAwards.get("PlayerID")));

        return memberAward;
    }

     /**
     * Get member profile from member_profile collection, and filter out unwanted fields. 
     */
    private Document getMemberProfile(MongoCollection<Document> memberProfileCollection, Object playerId) {
        Document memberAward = new Document();
        Document memberProfile = memberProfileCollection.find(new Document("player_id", playerId)).first();
        if (memberProfile != null) {
            memberAward.put("member_no", memberProfile.getString("member_no"));
            memberAward.put("member_profile", convertToSnakeCase(filterFields(extractFields(memberProfile, Set.of(
                    "is_active_program", "club_state", "club_state_name", "primary_host_id",
                    "secondary_host_id", "primary_host_num", "secondary_host_num",
                    "is_banned", "is_inactive")), Set.of("_id"))));
        }
        return memberAward;
    }

    /**
     * Determines the prize type based on tAwards and tHUBPromotionRuleOutCome.
     */
    private int calculatePrizeType(Document tAwards, MongoCollection<Document> tHUBPromotionRuleOutCome) {
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

            // Apply PrizeType conditions
            if (tranCodeID == 10 || tranCodeID == 11 || tranCodeID == 12) {
                prizeType = 2;
            } else if (tranCodeID == 4) {
                prizeType = 3;
            } else if (tAwards.containsKey("RID") && pidValues.contains(tAwards.get("RID"))) {
                prizeType = 4;
            }
        }
        return prizeType;
   }

    /**
     * Retrieves a document by a specific field, filters out unwanted fields,
     * and converts keys to snake_case.
     */
    private Document getFilteredAndConvertedDocument(MongoCollection<Document> collection, String field, Object value) {
        Document document = collection.find(new Document(field, value)).first();
        return convertToSnakeCase(filterFields(document, EXCLUDED_FIELDS));
    }

    /**
     * Retrieves a document with specific fields, filters only wanted fields,
     * and converts keys to snake_case.
     */
    private Document getFilteredAndConvertedDocument(MongoCollection<Document> collection, String field, Object value, Set<String> includeFields) {
        Document document = collection.find(new Document(field, value)).first();
        return convertToSnakeCase(filterFields(extractFields(document, includeFields), Set.of("_id")));
    }

    /**
     * Filters out unwanted fields from a MongoDB document.
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
     * Extracts only specific fields from a MongoDB document.
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
     * Converts field names from CamelCase to snake_case.
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
     * Converts a string from CamelCase to snake_case.
     */
    private String toSnakeCase(String camelCase) {
        return camelCase.replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                        .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
                        .toLowerCase();
    }
}
