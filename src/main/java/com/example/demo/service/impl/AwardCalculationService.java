package com.example.demo.service.impl;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.types.Decimal128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

@Service
public class AwardCalculationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(Task1.class);
    private final MongoClient mongoClient;
    private final String databaseName;
    private static final Set<String> EXCLUDED_FIELDS = Set.of("_id", "CreatedDtm", "CreatedBy", "ModifiedDtm", "DataRowVersion");
    public static final int ODS_SCHEMA_VERSION = 2;  // Hardcoded value
    private static final Decimal128 DECIMAL128_ZERO = new Decimal128(BigDecimal.ZERO);


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

        MongoCollection<Document> tPlayerStub = database.getCollection("hub_tPlayerStub");
        MongoCollection<Document> tPlayerPromo = database.getCollection("hub_tPlayerPromo");
        MongoCollection<Document> tPlayerPoints = database.getCollection("hub_tPlayerPoints");
        MongoCollection<Document> tPrize = database.getCollection("hub_tPrize");
        MongoCollection<Document> tPrizeLocnMapping = database.getCollection("hub_tPrizeLocnMapping");
        MongoCollection<Document> memberProfileCollection = database.getCollection("member_profile");
        MongoCollection<Document> tHUBPromotionRuleOutCome = database.getCollection("hub_tHUBPromotionRuleOutCome");
        MongoCollection<Document> tTranCode = database.getCollection("hub_tTranCode");

        // Convert tAwards fields to snake_case while filtering out unwanted fields
        Document memberAward = convertToSnakeCase(filterFields(tAwards, Set.of("_id")));

        // Populate related collections
        Document playerStub = getFilteredAndConvertedDocument(tPlayerStub, "TrainId", tAwards.get("TrainId"));
        memberAward.put("player_stub", playerStub);
        Document playerPromo = getFilteredAndConvertedDocument(tPlayerPromo, "TrainId", tAwards.get("TrainId"));
        memberAward.put("player_promo", playerPromo);
        Document tranCode = getFilteredAndConvertedDocument(tTranCode, "TranCodeId", tAwards.get("TranCodeId"), Set.of("TranCodeId", "TranCode", "TranName", "TranType", "ItemCode"));
        memberAward.put("tran_code", tranCode); 

        // Populate tPlayerPoints
        List<Document> playerPoints = tPlayerPoints.find(new Document("TranId", tAwards.get("TranId"))).into(new java.util.ArrayList<>());
        List<Document> filteredPlayerPoints = playerPoints.stream()
                .map(doc -> convertToSnakeCase(filterFields(doc, EXCLUDED_FIELDS)))
                .collect(Collectors.toList());
        memberAward.put("player_points", filteredPlayerPoints);

        // Populate tPrize
        Document prize = getFilteredAndConvertedDocument(tPrize, "PrizeId", tAwards.get("PrizeId"), Set.of("PrizeId", "PrizeCode", "PrizeName", "AwardCode","ExpiryDays", "ExpiryDate" ));
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
        memberAward.putAll(getMemberProfile(memberProfileCollection, tAwards.get("PlayerId")));

        // Calculate and add prize_amount
        memberAward.put("prize_amount", calculatePrizeAmount(tAwards, playerStub, playerPromo, playerPoints));

        // Calculate and add expire_date
        memberAward.put("expire_date", calculateExpireDate(prize, tAwards));


        memberAward.append("_ODS_modified_dtm", new Date())       // Always update modified timestamp
        .append("_ODS_is_deleted", false)        // Default value
        .append("_ODS_replay_switch", false)     // Default value
        .append("_ODS_schema_version", ODS_SCHEMA_VERSION);

        return memberAward;
    }


    private Decimal128 calculatePrizeAmount(Document tAwards, Document playerStub, Document playerPromo, List<Document> playerPoints) {
        int tranCodeID = getInt(tAwards, "TranCodeId", -1);
        LOGGER.info("Calculating prize amount,tranCodeId {} tAwards: {}",tranCodeID, tAwards );
    
        if (tranCodeID == 0 || tranCodeID == 1) {
            return getDecimal128(tAwards, "AuthAward", DECIMAL128_ZERO);
        } else if (tranCodeID == 2 && playerStub != null) {
            return sumDecimal128(
                    getDecimal128(playerStub, "adj_stubs_cr", DECIMAL128_ZERO),
                    getDecimal128(playerStub, "adj_stubs_dr", DECIMAL128_ZERO),
                    getDecimal128(playerStub, "base_stubs", DECIMAL128_ZERO),
                    getDecimal128(playerStub, "bonus_stubs", DECIMAL128_ZERO),
                    getDecimal128(playerStub, "redeem_stubs", DECIMAL128_ZERO)
            );
        } else if (tranCodeID == 3 && playerPromo != null) {
            return sumDecimal128(
                    getDecimal128(playerPromo, "promo1", DECIMAL128_ZERO),
                    getDecimal128(playerPromo, "bonus_promo1", DECIMAL128_ZERO),
                    getDecimal128(playerPromo, "promo1_used", DECIMAL128_ZERO),
                    getDecimal128(playerPromo, "adj_promo1_cr", DECIMAL128_ZERO),
                    getDecimal128(playerPromo, "adj_promo1_dr", DECIMAL128_ZERO)
            );
        } else if (tranCodeID == 4 && playerPoints != null) {
            return playerPoints.stream()
                    .map(doc -> sumDecimal128(
                            getDecimal128(doc, "game_pts", DECIMAL128_ZERO),
                            getDecimal128(doc, "base_pts", DECIMAL128_ZERO),
                            getDecimal128(doc, "bonus_pts", DECIMAL128_ZERO),
                            getDecimal128(doc, "adj_pts_cr", DECIMAL128_ZERO),
                            getDecimal128(doc, "adj_pts_dr", DECIMAL128_ZERO),
                            getDecimal128(doc, "redeem_pts", DECIMAL128_ZERO)
                    ))
                    .reduce(DECIMAL128_ZERO, this::sumDecimal128);
        }
    
        return DECIMAL128_ZERO;
    }
    

    private Decimal128 getDecimal128(Document doc, String key, Decimal128 defaultValue) {
        if (doc == null || !doc.containsKey(key)) return defaultValue;
    
        Object value = doc.get(key);
    
        if (value instanceof Decimal128) {
            return (Decimal128) value;
        } else if (value instanceof Integer) {
            return new Decimal128(BigDecimal.valueOf((Integer) value));
        } else if (value instanceof Long) {
            return new Decimal128(BigDecimal.valueOf((Long) value));
        } else if (value instanceof Double) {
            return new Decimal128(BigDecimal.valueOf((Double) value));
        } else if (value instanceof Number) {
            return new Decimal128(BigDecimal.valueOf(((Number) value).doubleValue()));
        }
    
        return defaultValue;
    }

    private Decimal128 sumDecimal128(Decimal128... values) {
        BigDecimal sum = BigDecimal.ZERO;
        for (Decimal128 val : values) {
            sum = sum.add(val.bigDecimalValue());
        }
        return new Decimal128(sum);
    }

    private int getInt(Document doc, String key, int defaultValue) {
        Object value = doc.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return defaultValue;
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
        int tranCodeID = tAwards.getInteger("TranCodeId", -1);

        // Retrieve targetRID from tHUBPromotionRuleOutCome where PlayerID = tAwards.PlayerID
        Document hub = tHUBPromotionRuleOutCome.find(new Document("PlayerID", tAwards.get("PlayerId"))).first();
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

    private Date calculateExpireDate(Document prize, Document tAwards) {
        if (prize == null || tAwards == null) return null;

        int expiryDays = prize.getInteger("expiry_days", 0);
        Date awardDate = tAwards.getDate("date");
        Date expiryDate = prize.getDate("expiry_date");

        LOGGER.info("in cal expirydate prize {}, tawards, {} prize.expiryDays {}, awards.awardDate {}, prize.expiryDate {}",prize,tAwards, expiryDays, awardDate, expiryDate);

        if (expiryDays > 0 && awardDate != null) {
            LocalDate awardLocalDate = awardDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            return Date.from(awardLocalDate.plusDays(expiryDays).atStartOfDay(ZoneId.systemDefault()).toInstant());
        }

        return expiryDate;
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
            if (!excludeFields.contains(key) && !key.startsWith("_ODS")) {
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
