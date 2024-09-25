package com.example.demo;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.example.demo.service.ResumeTokenService;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;

public class ResumeTokenServiceTest {

        @Mock
        private MongoCollection<Document> resumeTokenCollection;

        @Mock
        private FindIterable<Document> findIterable;

        @InjectMocks
        private ResumeTokenService resumeTokenService;

        @BeforeEach
        public void setUp() {
                MockitoAnnotations.openMocks(this);
                when(resumeTokenCollection.find()).thenReturn(findIterable);
        }

        @Test
        public void testSaveResumeToken() {
                BsonDocument resumeToken = new BsonDocument();

                // Call the method under test
                resumeTokenService.saveResumeToken(resumeToken, "TestThread");

                // Explicitly specify the argument types to resolve ambiguity
                verify(resumeTokenCollection, times(1)).updateOne(
                                any(Bson.class), // Argument type for the filter
                                any(Document.class), // Argument type for the update document
                                any(UpdateOptions.class) // Argument type for the UpdateOptions
                );
        }

        @Test
        public void testGetLatestResumeToken() {
                Document tokenDoc = new Document("resumeToken", new Document("_data", "tokenData"));
                when(findIterable.sort(any())).thenReturn(findIterable);
                when(findIterable.first()).thenReturn(tokenDoc);

                resumeTokenService.getLatestResumeToken();

                verify(resumeTokenCollection, times(1)).find();
        }
}
