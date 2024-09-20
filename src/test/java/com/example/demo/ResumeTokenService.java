package com.example.demo;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.example.demo.service.ResumeTokenService;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

class ResumeTokenServiceTest {

        @Mock
        private MongoCollection<Document> mockCollection;

        @Mock
        private FindIterable<Document> mockFindIterable;

        private ResumeTokenService resumeTokenService;

        @BeforeEach
        void setUp() {
                MockitoAnnotations.openMocks(this);
                resumeTokenService = new ResumeTokenService(mockCollection);
        }

        @Test
        void testSaveResumeToken() {
                BsonDocument resumeToken = new BsonDocument();
                int threadId = 1;

                resumeTokenService.saveResumeToken(resumeToken, threadId);

                // Verify the updateOne method is called correctly
                verify(mockCollection).updateOne(eq(Filters.eq("threadID", threadId)), any(Document.class),
                                any(UpdateOptions.class));
        }

        @Test
        void testGetLatestResumeToken() {
                // Mock behavior for find() and sort() methods
                when(mockCollection.find()).thenReturn(mockFindIterable);
                when(mockFindIterable.sort(any(Document.class))).thenReturn(mockFindIterable);
                when(mockFindIterable.first()).thenReturn(null); // Simulate no document found scenario

                // Test retrieval
                BsonDocument resumeToken = resumeTokenService.getLatestResumeToken();

                // Assert the retrieved resume token is null
                assertNull(resumeToken);
        }
}
