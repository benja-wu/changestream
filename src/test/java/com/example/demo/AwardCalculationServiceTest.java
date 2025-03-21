package com.example.demo;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
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
    @Mock private MongoCollection<Document> tTranCodeCollection;

    @InjectMocks private AwardCalculationService awardCalculationService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        awardCalculationService = new AwardCalculationService(mongoClient, "testDatabase");
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection("hub_tPlayerStub")).thenReturn(tPlayerStubCollection);
        when(mongoDatabase.getCollection("hub_tPlayerPromo")).thenReturn(tPlayerPromoCollection);
        when(mongoDatabase.getCollection("hub_tPlayerPoints")).thenReturn(tPlayerPointsCollection);
        when(mongoDatabase.getCollection("hub_tPrize")).thenReturn(tPrizeCollection);
        when(mongoDatabase.getCollection("hub_tPrizeLocnMapping")).thenReturn(tPrizeLocnMappingCollection);
        when(mongoDatabase.getCollection("member_profile")).thenReturn(memberProfileCollection);
        when(mongoDatabase.getCollection("hub_tHUBPromotionRuleOutCome")).thenReturn(tHUBPromotionRuleOutComeCollection);
        when(mongoDatabase.getCollection("hub_tTranCode")).thenReturn(tTranCodeCollection);
    }

    @Test
    void testCalculateAward() {
        Document tAwards = new Document("TrainId", "TR123456")
                .append("TranId", "10002877609")
                .append("PrizeId", 1210017247)
                .append("PlayerId", 777777777) // Match PlayerId as used in calculatePrizeType
                .append("TranCodeId", 10)
                .append("Doc", "P");

        // Mock all collections
        mockSingleDoc(tPlayerStubCollection, "TrainId", "TR123456", new Document("TrainId", "TR123456").append("StubValue", 10));
        mockSingleDoc(tPlayerPromoCollection, "TrainId", "TR123456", new Document("TrainId", "TR123456").append("PromoValue", 20));
        mockListDocs(tPlayerPointsCollection, "TranId", "10002877609", Arrays.asList(
                new Document("TranId", "10002877609").append("Pts", 50),
                new Document("TranId", "10002877609").append("Pts", 30)
        ));
        mockSingleDoc(tPrizeCollection, "PrizeId", 1210017247, new Document("PrizeId", 1210017247)
                .append("PrizeCode", "ABCDEFG")
                .append("PrizeName", "Test Prize"));
        mockSingleDoc(tPrizeLocnMappingCollection, "PrizeId", 1210017247, new Document("PrizeId", 1210017247).append("CasinoId", 110000002));
        mockSingleDoc(memberProfileCollection, "player_id", 777777777, new Document("player_id", 777777777).append("member_no", "888888888"));
        mockSingleDoc(tTranCodeCollection, "TranCodeId", 10, new Document("TranCodeId", 10)
                .append("TranCode", "TC001")
                .append("TranName", "Transaction Name")
                .append("TranType", "TypeA")
                .append("ItemCode", "IC001"));
        mockSingleDoc(tHUBPromotionRuleOutComeCollection, "PlayerID", 777777777, new Document("PlayerID", 777777777).append("RID", "RID12345"));
        mockListDocs(tHUBPromotionRuleOutComeCollection, "RID", "RID12345", Arrays.asList());

        Document result = awardCalculationService.calculateAward(tAwards);

        assertNotNull(result);
        assertEquals("TR123456", result.getString("train_id"));
        assertEquals("10002877609", result.getString("tran_id"));
        assertEquals(2, result.getInteger("award_prize_type"));
        assertEquals(true, result.getBoolean("is_doc_pmprize"));
        assertEquals("888888888", result.getString("member_no"));
        assertTrue(result.containsKey("player_stub"));
        assertTrue(result.containsKey("player_promo"));
        assertTrue(result.containsKey("player_points"));
        assertTrue(result.containsKey("prize_locn_mapping"));
        Document tranCodeResult = (Document) result.get("tran_code");
        assertNotNull(tranCodeResult);
        assertEquals(10, tranCodeResult.getInteger("tran_code_id"));
    }

    @Test
    void testCalculateAwardPrizeTypeTranCode11() {
        Document tAwards = new Document("TrainId", "TR123456")
                .append("TranId", "10002877609")
                .append("PrizeId", 1210017247)
                .append("PlayerId", 777777777) // Match PlayerId
                .append("TranCodeId", 11)
                .append("Doc", "P");

        // Mock all collections
        mockSingleDoc(tPlayerStubCollection, "TrainId", "TR123456", new Document("TrainId", "TR123456"));
        mockSingleDoc(tPlayerPromoCollection, "TrainId", "TR123456", new Document("TrainId", "TR123456"));
        mockListDocs(tPlayerPointsCollection, "TranId", "10002877609", Arrays.asList(new Document("TranId", "10002877609")));
        mockSingleDoc(tPrizeCollection, "PrizeId", 1210017247, new Document("PrizeId", 1210017247));
        mockSingleDoc(tPrizeLocnMappingCollection, "PrizeId", 1210017247, new Document("PrizeId", 1210017247));
        mockSingleDoc(memberProfileCollection, "player_id", 777777777, new Document("player_id", 777777777).append("member_no", "M123"));
        mockSingleDoc(tTranCodeCollection, "TranCodeId", 11, new Document("TranCodeId", 11));
        mockSingleDoc(tHUBPromotionRuleOutComeCollection, "PlayerID", 777777777, new Document("PlayerID", 777777777).append("RID", "RID12345"));
        mockListDocs(tHUBPromotionRuleOutComeCollection, "RID", "RID12345", Arrays.asList());

        Document result = awardCalculationService.calculateAward(tAwards);

        assertNotNull(result);
        assertEquals(2, result.getInteger("award_prize_type"));
    }

    @Test
    void testCalculateAwardPrizeTypeTranCode12() {
        Document tAwards = new Document("TrainId", "TR123456")
                .append("TranId", "10002877609")
                .append("PrizeId", 1210017247)
                .append("PlayerId", 777777777) // Match PlayerId
                .append("TranCodeId", 12)
                .append("Doc", "P");

        // Mock all collections
        mockSingleDoc(tPlayerStubCollection, "TrainId", "TR123456", new Document("TrainId", "TR123456"));
        mockSingleDoc(tPlayerPromoCollection, "TrainId", "TR123456", new Document("TrainId", "TR123456"));
        mockListDocs(tPlayerPointsCollection, "TranId", "10002877609", Arrays.asList(new Document("TranId", "10002877609")));
        mockSingleDoc(tPrizeCollection, "PrizeId", 1210017247, new Document("PrizeId", 1210017247));
        mockSingleDoc(tPrizeLocnMappingCollection, "PrizeId", 1210017247, new Document("PrizeId", 1210017247));
        mockSingleDoc(memberProfileCollection, "player_id", 777777777, new Document("player_id", 777777777).append("member_no", "M123"));
        mockSingleDoc(tTranCodeCollection, "TranCodeId", 12, new Document("TranCodeId", 12));
        mockSingleDoc(tHUBPromotionRuleOutComeCollection, "PlayerID", 777777777, new Document("PlayerID", 777777777).append("RID", "RID12345"));
        mockListDocs(tHUBPromotionRuleOutComeCollection, "RID", "RID12345", Arrays.asList());

        Document result = awardCalculationService.calculateAward(tAwards);

        assertNotNull(result);
        assertEquals(2, result.getInteger("award_prize_type"));
    }

    @Test
    void testCalculateAwardPrizeTypeTranCode4() {
        Document tAwards = new Document("TrainId", "TR123456")
                .append("TranId", "10002877609")
                .append("PrizeId", 1210017247)
                .append("PlayerId", 777777777) // Match PlayerId
                .append("TranCodeId", 4)
                .append("Doc", "P");

        // Mock all collections
        mockSingleDoc(tPlayerStubCollection, "TrainId", "TR123456", new Document("TrainId", "TR123456"));
        mockSingleDoc(tPlayerPromoCollection, "TrainId", "TR123456", new Document("TrainId", "TR123456"));
        mockListDocs(tPlayerPointsCollection, "TranId", "10002877609", Arrays.asList(new Document("TranId", "10002877609")));
        mockSingleDoc(tPrizeCollection, "PrizeId", 1210017247, new Document("PrizeId", 1210017247));
        mockSingleDoc(tPrizeLocnMappingCollection, "PrizeId", 1210017247, new Document("PrizeId", 1210017247));
        mockSingleDoc(memberProfileCollection, "player_id", 777777777, new Document("player_id", 777777777).append("member_no", "M123"));
        mockSingleDoc(tTranCodeCollection, "TranCodeId", 4, new Document("TranCodeId", 4));
        mockSingleDoc(tHUBPromotionRuleOutComeCollection, "PlayerID", 777777777, new Document("PlayerID", 777777777).append("RID", "RID12345"));
        mockListDocs(tHUBPromotionRuleOutComeCollection, "RID", "RID12345", Arrays.asList());

        Document result = awardCalculationService.calculateAward(tAwards);

        assertNotNull(result);
        assertEquals(3, result.getInteger("award_prize_type"));
    }

    @Test
    void testCalculateAwardPrizeType4() {
        Document tAwards = new Document("TrainId", "TR123456")
                .append("TranId", "10002877609")
                .append("PrizeId", 1210017247)
                .append("PlayerId", 777777777) // Match PlayerId
                .append("TranCodeId", 5)
                .append("Doc", "P")
                .append("RID", "PID123");

        // Mock all collections
        mockSingleDoc(tPlayerStubCollection, "TrainId", "TR123456", new Document("TrainId", "TR123456"));
        mockSingleDoc(tPlayerPromoCollection, "TrainId", "TR123456", new Document("TrainId", "TR123456"));
        mockListDocs(tPlayerPointsCollection, "TranId", "10002877609", Arrays.asList(new Document("TranId", "10002877609")));
        mockSingleDoc(tPrizeCollection, "PrizeId", 1210017247, new Document("PrizeId", 1210017247));
        mockSingleDoc(tPrizeLocnMappingCollection, "PrizeId", 1210017247, new Document("PrizeId", 1210017247));
        mockSingleDoc(memberProfileCollection, "player_id", 777777777, new Document("player_id", 777777777).append("member_no", "M123"));
        mockSingleDoc(tTranCodeCollection, "TranCodeId", 5, new Document("TranCodeId", 5));
        mockSingleDoc(tHUBPromotionRuleOutComeCollection, "PlayerID", 777777777, new Document("PlayerID", 777777777).append("RID", "targetRID"));
        mockListDocs(tHUBPromotionRuleOutComeCollection, "RID", "targetRID", Arrays.asList(
                new Document("PID", "PID123"),
                new Document("PID", "PID456")
        ));

        Document result = awardCalculationService.calculateAward(tAwards);

        assertNotNull(result);
        assertEquals(4, result.getInteger("award_prize_type"));
    }

    @Test
    void testToSnakeCase() throws Exception {
        Method method = AwardCalculationService.class.getDeclaredMethod("toSnakeCase", String.class);
        method.setAccessible(true);
        assertEquals("prize_code", method.invoke(awardCalculationService, "PrizeCode"));
        assertEquals("prize_name", method.invoke(awardCalculationService, "PrizeName"));
        assertEquals("award_code", method.invoke(awardCalculationService, "AwardCode"));
        assertEquals("promotion_msg_id", method.invoke(awardCalculationService, "PromotionMsgId"));
        assertEquals("created_dtm", method.invoke(awardCalculationService, "CreatedDtm"));
        assertEquals("modified_by", method.invoke(awardCalculationService, "ModifiedBy"));
        assertEquals("is_max_prizes_per_trip", method.invoke(awardCalculationService, "IsMaxPrizesPerTrip"));
        assertEquals("_ods_replay_switch", method.invoke(awardCalculationService, "_ODS_replay_switch"));
        assertEquals("hub_promotion_id", method.invoke(awardCalculationService, "HUBPromotionID"));
    }

    // Updated helper methods to mock collections
    private void mockSingleDoc(MongoCollection<Document> collection, String field, Object value, Document doc) {
        FindIterable<Document> iterable = mock(FindIterable.class);
        when(iterable.first()).thenReturn(doc);
        when(collection.find(new Document(field, value))).thenReturn(iterable); // Ensure find() returns iterable
    }

    private void mockListDocs(MongoCollection<Document> collection, String field, Object value, List<Document> docs) {
        FindIterable<Document> iterable = mock(FindIterable.class);
        when(iterable.into(any())).thenReturn(docs);
        when(collection.find(new Document(field, value))).thenReturn(iterable); // Ensure find() returns iterable

        MongoCursor<Document> cursor = mock(MongoCursor.class);
        when(iterable.iterator()).thenReturn(cursor);
        if (docs.isEmpty()) {
            when(cursor.hasNext()).thenReturn(false);
        } else {
            Boolean[] hasNextSequence = new Boolean[docs.size() + 1];
            for (int i = 0; i < docs.size(); i++) hasNextSequence[i] = true;
            hasNextSequence[docs.size()] = false;
            when(cursor.hasNext()).thenReturn(true, hasNextSequence);
            when(cursor.next()).thenReturn(docs.get(0), docs.toArray(new Document[0]));
        }
    }
}