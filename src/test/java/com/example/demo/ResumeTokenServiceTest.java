package com.example.demo;

import java.util.Map;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import com.example.demo.service.ResumeTokenService;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;

public class ResumeTokenServiceTest {

    @Mock
    private Map<String, MongoCollection<Document>> collectionMap;

    @Mock
    private MongoCollection<Document> resumeTokenCollection;

    @Mock
    private FindIterable<Document> findIterable;

    private static final String RESUME_TOKEN_COLLECTION_NAME = "resumeTokenCollection";

    @InjectMocks
    private ResumeTokenService resumeTokenService;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // ✅ Ensure `resumeTokenCollectionName` is correctly set in the service
        resumeTokenService = new ResumeTokenService(collectionMap, RESUME_TOKEN_COLLECTION_NAME);

        // ✅ Mock `collectionMap.get("resumeTokenCollection")` correctly
        when(collectionMap.get(eq(RESUME_TOKEN_COLLECTION_NAME))).thenReturn(resumeTokenCollection);

        // ✅ Explicitly specify `find(any(Bson.class))` to resolve ambiguity
        when(resumeTokenCollection.find(any(Bson.class))).thenReturn(findIterable);
        when(findIterable.sort(any(Bson.class))).thenReturn(findIterable);
    }

    @Test
    public void testSaveResumeToken() {
        BsonDocument resumeToken = new BsonDocument();
        BsonTimestamp timestamp = new BsonTimestamp();
        String threadName = "TestThread";
        String collectionName = "testCollection";

        // ✅ Call the method under test
        resumeTokenService.saveResumeToken(timestamp, resumeToken, threadName, collectionName);

        // ✅ Verify `updateOne()` was called correctly
        verify(resumeTokenCollection, times(1)).updateOne(
                any(Bson.class), // Filter
                any(Document.class), // Update
                any(UpdateOptions.class) // Options
        );
    }

    @Test
    public void testGetLatestResumeToken() {
        // ✅ Mock token retrieval
        Document tokenDoc = new Document("resumeToken", new Document("_data", "tokenData"));
        when(findIterable.first()).thenReturn(tokenDoc);

        // ✅ Call method
        resumeTokenService.getResumeToken("testCollection");

        // ✅ Verify `collectionMap.get("resumeTokenCollection")` was invoked
        verify(collectionMap, times(1)).get(eq(RESUME_TOKEN_COLLECTION_NAME));
        verify(resumeTokenCollection, times(1)).find(any(Bson.class));
    }
}
