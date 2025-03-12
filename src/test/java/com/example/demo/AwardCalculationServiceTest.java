package com.example.demo;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import com.example.demo.service.impl.AwardCalculationService;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

class AwardCalculationServiceTest {

    @Mock private MongoClient mongoClient;
    @Mock private MongoDatabase mongoDatabase;
    @Mock private MongoCollection<Document> tPlayerStubCollection;
    @Mock private MongoCollection<Document> tPlayerPromoCollection;
    @Mock private MongoCollection<Document> tPlayerPointsCollection;
    @Mock private MongoCollection<Document> tPrizeCollection;
    @Mock private MongoCollection<Document> tPrizeLocnMappingCollection;
    @Mock private MongoCollection<Document> memberProfileCollection;
    @Mock private MongoCollection<Document> tHUBPromotionRuleOutComeCollection;

    @InjectMocks private AwardCalculationService awardCalculationService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        awardCalculationService = new AwardCalculationService(mongoClient, "testDatabase"); // Inject database name
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection("tPlayerStub")).thenReturn(tPlayerStubCollection);
        when(mongoDatabase.getCollection("tPlayerPromo1")).thenReturn(tPlayerPromoCollection);
        when(mongoDatabase.getCollection("tPlayerPoints")).thenReturn(tPlayerPointsCollection);
        when(mongoDatabase.getCollection("tPrize")).thenReturn(tPrizeCollection);
        when(mongoDatabase.getCollection("tPrizeLocnMapping")).thenReturn(tPrizeLocnMappingCollection);
        when(mongoDatabase.getCollection("member_profile")).thenReturn(memberProfileCollection);
        when(mongoDatabase.getCollection("tHUBPromotionRuleOutCome")).thenReturn(tHUBPromotionRuleOutComeCollection);
    }

    @Test
    void testCalculateAward() {
        // Mock input document (tAwards)
        Document tAwards = new Document("TrainId", "TR123456")
                .append("TranId", "10002877609")
                .append("PrizeId", 1210017247)
                .append("PlayerID", 777777777)
                .append("TranCodeID", 10) // Changed to 10 to match prize_type = 2 logic
                .append("Doc", "P");
    
        // Set up mock FindIterable for each collection individually
    
        // tPlayerStubCollection
        Document stubDoc = new Document("TrainId", "TR123456").append("StubValue", 10);
        FindIterable<Document> stubIterable = mock(FindIterable.class);
        when(stubIterable.first()).thenReturn(stubDoc);
        when(tPlayerStubCollection.find(any(Bson.class))).thenReturn(stubIterable);
    
        // tPlayerPromoCollection
        Document promoDoc = new Document("TrainId", "TR123456").append("PromoValue", 20);
        FindIterable<Document> promoIterable = mock(FindIterable.class);
        when(promoIterable.first()).thenReturn(promoDoc);
        when(tPlayerPromoCollection.find(any(Bson.class))).thenReturn(promoIterable);
    
        // tPlayerPointsCollection (uses .into(), so set up iterator)
        List<Document> pointsDocs = Arrays.asList(
                new Document("TranId", "10002877609").append("Pts", 50),
                new Document("TranId", "10002877609").append("Pts", 30)
        );
        FindIterable<Document> pointsIterable = mock(FindIterable.class);
        MongoCursor<Document> pointsCursor = mock(MongoCursor.class);
        when(pointsIterable.iterator()).thenReturn(pointsCursor);
        when(pointsCursor.hasNext()).thenReturn(true, true, false); // Two documents, then done
        when(pointsCursor.next()).thenReturn(pointsDocs.get(0), pointsDocs.get(1));
        when(tPlayerPointsCollection.find(any(Bson.class))).thenReturn(pointsIterable);
    
        // tPrizeCollection
        Document prizeDoc = new Document("PrizeId", 1210017247)
                .append("PrizeCode", "ABCDEFG")
                .append("PrizeName", "Test Prize");
        FindIterable<Document> prizeIterable = mock(FindIterable.class);
        when(prizeIterable.first()).thenReturn(prizeDoc);
        when(tPrizeCollection.find(any(Bson.class))).thenReturn(prizeIterable);
    
        // tPrizeLocnMappingCollection
        Document locnDoc = new Document("PrizeId", 1210017247).append("CasinoId", 110000002);
        FindIterable<Document> locnIterable = mock(FindIterable.class);
        when(locnIterable.first()).thenReturn(locnDoc);
        when(tPrizeLocnMappingCollection.find(any(Bson.class))).thenReturn(locnIterable);
    
        // memberProfileCollection
        Document memberDoc = new Document("player_id", 777777777).append("member_no", "888888888");
        FindIterable<Document> memberIterable = mock(FindIterable.class);
        when(memberIterable.first()).thenReturn(memberDoc);
        when(memberProfileCollection.find(any(Bson.class))).thenReturn(memberIterable);
    
        // tHUBPromotionRuleOutComeCollection (handles two finds: PlayerID and RID)
        // First find: PlayerID = 777777777
        Document hubDoc = new Document("PlayerID", 777777777).append("RID", "RID12345");
        FindIterable<Document> hubIterable1 = mock(FindIterable.class);
        when(hubIterable1.first()).thenReturn(hubDoc);
        // Second find: RID = "RID12345" (simplified for test; adjust if PIDs are needed)
        FindIterable<Document> hubIterable2 = mock(FindIterable.class);
        when(hubIterable2.iterator()).thenReturn(mock(MongoCursor.class)); // Empty iterator for simplicity
        // Return different iterables for consecutive calls
        when(tHUBPromotionRuleOutComeCollection.find(any(Bson.class)))
                .thenReturn(hubIterable1) // First call (PlayerID)
                .thenReturn(hubIterable2); // Second call (RID)
    
        // Execute function
        Document result = awardCalculationService.calculateAward(tAwards);
    
        // Assertions
        assertNotNull(result);
        assertEquals("TR123456", result.getString("train_id"));
        assertEquals("10002877609", result.getString("tran_id"));
        assertEquals(2, result.getInteger("award_prize_type")); // Matches TranCodeID = 10 logic
        //assertEquals("ABCDEFG", result.getString("prize_code"));
        //assertEquals("Test Prize", result.getString("prize_name"));
        assertEquals(true, result.getBoolean("is_doc_pmprize"));
        assertEquals("888888888", result.getString("member_no"));
        assertTrue(result.containsKey("player_stub"));
        assertTrue(result.containsKey("player_promo1"));
        assertTrue(result.containsKey("player_points"));
        assertTrue(result.containsKey("prize_locn_mapping"));
    } 

    @Test
    void testCalculateAwardPrizeTypeTranCode11() {
        // Mock tAwards
        Document tAwards = new Document("TrainId", "TR123456")
                .append("TranId", "10002877609")
                .append("PrizeId", 1210017247)
                .append("PlayerID", 777777777)
                .append("TranCodeID", 11)
                .append("Doc", "P");

        // Mock tHUBPromotionRuleOutComeCollection
        Document hubDoc = new Document("PlayerID", 777777777).append("RID", "RID12345");
        FindIterable<Document> hubIterable = mock(FindIterable.class);
        when(hubIterable.first()).thenReturn(hubDoc);
        FindIterable<Document> pidIterable = mock(FindIterable.class);
        when(pidIterable.into(any())).thenReturn(Arrays.asList());
        when(tHUBPromotionRuleOutComeCollection.find(new Document("PlayerID", 777777777))).thenReturn(hubIterable);
        when(tHUBPromotionRuleOutComeCollection.find(new Document("RID", "RID12345"))).thenReturn(pidIterable);

        // Minimal mocks for other collections
        mockCollection(tPlayerStubCollection, "TrainId", "TR123456");
        mockCollection(tPlayerPromoCollection, "TrainId", "TR123456");
        mockCollection(tPlayerPointsCollection, "TranId", "10002877609", true);
        mockCollection(tPrizeCollection, "PrizeId", 1210017247);
        mockCollection(tPrizeLocnMappingCollection, "PrizeId", 1210017247);
        mockCollection(memberProfileCollection, "player_id", 777777777);

        // Execute
        Document result = awardCalculationService.calculateAward(tAwards);

        // Assert
        assertNotNull(result);
        assertEquals(2, result.getInteger("award_prize_type"));
    }

    @Test
    void testCalculateAwardPrizeTypeTranCode12() {
        Document tAwards = new Document("TrainId", "TR123456")
                .append("TranId", "10002877609")
                .append("PrizeId", 1210017247)
                .append("PlayerID", 777777777)
                .append("TranCodeID", 12)
                .append("Doc", "P");

        Document hubDoc = new Document("PlayerID", 777777777).append("RID", "RID12345");
        FindIterable<Document> hubIterable = mock(FindIterable.class);
        when(hubIterable.first()).thenReturn(hubDoc);
        FindIterable<Document> pidIterable = mock(FindIterable.class);
        when(pidIterable.into(any())).thenReturn(Arrays.asList());
        when(tHUBPromotionRuleOutComeCollection.find(new Document("PlayerID", 777777777))).thenReturn(hubIterable);
        when(tHUBPromotionRuleOutComeCollection.find(new Document("RID", "RID12345"))).thenReturn(pidIterable);

        mockCollection(tPlayerStubCollection, "TrainId", "TR123456");
        mockCollection(tPlayerPromoCollection, "TrainId", "TR123456");
        mockCollection(tPlayerPointsCollection, "TranId", "10002877609", true);
        mockCollection(tPrizeCollection, "PrizeId", 1210017247);
        mockCollection(tPrizeLocnMappingCollection, "PrizeId", 1210017247);
        mockCollection(memberProfileCollection, "player_id", 777777777);

        Document result = awardCalculationService.calculateAward(tAwards);

        assertNotNull(result);
        assertEquals(2, result.getInteger("award_prize_type"));
    }

    @Test
    void testCalculateAwardPrizeTypeTranCode4() {
        Document tAwards = new Document("TrainId", "TR123456")
                .append("TranId", "10002877609")
                .append("PrizeId", 1210017247)
                .append("PlayerID", 777777777)
                .append("TranCodeID", 4)
                .append("Doc", "P");

        Document hubDoc = new Document("PlayerID", 777777777).append("RID", "RID12345");
        FindIterable<Document> hubIterable = mock(FindIterable.class);
        when(hubIterable.first()).thenReturn(hubDoc);
        FindIterable<Document> pidIterable = mock(FindIterable.class);
        when(pidIterable.into(any())).thenReturn(Arrays.asList());
        when(tHUBPromotionRuleOutComeCollection.find(new Document("PlayerID", 777777777))).thenReturn(hubIterable);
        when(tHUBPromotionRuleOutComeCollection.find(new Document("RID", "RID12345"))).thenReturn(pidIterable);

        mockCollection(tPlayerStubCollection, "TrainId", "TR123456");
        mockCollection(tPlayerPromoCollection, "TrainId", "TR123456");
        mockCollection(tPlayerPointsCollection, "TranId", "10002877609", true);
        mockCollection(tPrizeCollection, "PrizeId", 1210017247);
        mockCollection(tPrizeLocnMappingCollection, "PrizeId", 1210017247);
        mockCollection(memberProfileCollection, "player_id", 777777777);

        Document result = awardCalculationService.calculateAward(tAwards);

        assertNotNull(result);
        assertEquals(3, result.getInteger("award_prize_type"));
    }

    @Test
    void testCalculateAwardPrizeType4() {
        Document tAwards = new Document("TrainId", "TR123456")
                .append("TranId", "10002877609")
                .append("PrizeId", 1210017247)
                .append("PlayerID", 777777777)
                .append("TranCodeID", 5)
                .append("Doc", "P")
                .append("RID", "PID123");

        Document hubDoc = new Document("PlayerID", 777777777).append("RID", "targetRID");
        FindIterable<Document> hubIterable = mock(FindIterable.class);
        when(hubIterable.first()).thenReturn(hubDoc);
        List<Document> pidDocs = Arrays.asList(
                new Document("PID", "PID123"),
                new Document("PID", "PID456")
        );
        FindIterable<Document> pidIterable = mock(FindIterable.class);
        when(pidIterable.into(any())).thenReturn(pidDocs);
        when(tHUBPromotionRuleOutComeCollection.find(new Document("PlayerID", 777777777))).thenReturn(hubIterable);
        when(tHUBPromotionRuleOutComeCollection.find(new Document("RID", "targetRID"))).thenReturn(pidIterable);

        mockCollection(tPlayerStubCollection, "TrainId", "TR123456");
        mockCollection(tPlayerPromoCollection, "TrainId", "TR123456");
        mockCollection(tPlayerPointsCollection, "TranId", "10002877609", true);
        mockCollection(tPrizeCollection, "PrizeId", 1210017247);
        mockCollection(tPrizeLocnMappingCollection, "PrizeId", 1210017247);
        mockCollection(memberProfileCollection, "player_id", 777777777);

        Document result = awardCalculationService.calculateAward(tAwards);

        assertNotNull(result);
        assertEquals(4, result.getInteger("award_prize_type"));
    }

    private void mockCollection(MongoCollection<Document> collection, String field, Object value, boolean isList) {
        if (isList) {
            List<Document> docs = Arrays.asList(new Document(field, value));
            FindIterable<Document> iterable = mock(FindIterable.class);
            MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(iterable.iterator()).thenReturn(cursor);
            when(cursor.hasNext()).thenReturn(true, false);
            when(cursor.next()).thenReturn(docs.get(0));
            when(collection.find(any(Bson.class))).thenReturn(iterable);
        } else {
            Document doc = new Document(field, value);
            FindIterable<Document> iterable = mock(FindIterable.class);
            when(iterable.first()).thenReturn(doc);
            when(collection.find(any(Bson.class))).thenReturn(iterable);
        }
    }



    private void mockCollection(MongoCollection<Document> collection, String field, Object value) {
        mockCollection(collection, field, value, false);
    }

    @BeforeEach
    void setUpToSnakeCase() {
        awardCalculationService = new AwardCalculationService(null, "testDatabase");
    }

    @Test
    void testToSnakeCase() throws Exception {
        Method method = AwardCalculationService.class.getDeclaredMethod("toSnakeCase", String.class);
        method.setAccessible(true);
        System.out.printf("in the testToSnakeCase function\n");
        assertEquals("prize_code", method.invoke(awardCalculationService, "PrizeCode"));
        assertEquals("prize_name", method.invoke(awardCalculationService, "PrizeName"));
        assertEquals("award_code", method.invoke(awardCalculationService, "AwardCode"));
        assertEquals("promotion_msg_id", method.invoke(awardCalculationService, "PromotionMsgId"));
        assertEquals("created_dtm", method.invoke(awardCalculationService, "CreatedDtm"));
        assertEquals("modified_by", method.invoke(awardCalculationService, "ModifiedBy"));
        assertEquals("is_max_prizes_per_trip", method.invoke(awardCalculationService, "IsMaxPrizesPerTrip"));
        assertEquals("_ods_replay_switch", method.invoke(awardCalculationService, "_ODS_replay_switch"));
        assertEquals("hub_promotion_id", method.invoke(awardCalculationService, "HUBPromotionID"));
        System.out.printf("the test case result: %s\n", method.invoke(awardCalculationService, "HUBPromotionID"));
    }


  
    
}
