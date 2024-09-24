// ResumeTokenServiceTest.java
package com.example.demo;

import java.util.Date;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import com.example.demo.service.ResumeTokenService;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

class ResumeTokenServiceTest {

        @Mock
        private MongoCollection<Document> resumeTokenCollection;

        @InjectMocks
        private ResumeTokenService resumeTokenService;

        @BeforeEach
        void setUp() {
                MockitoAnnotations.openMocks(this);
        }

        @Test
        void testSaveResumeToken() {
                // Create a mock resume token and expected document
                BsonDocument resumeToken = BsonDocument.parse("{'_data': 'test'}");
                Document expectedDocument = new Document()
                                .append("threadName", "testThread")
                                .append("resumeToken", resumeToken)
                                .append("date", new Date())
                                .append("appName", "demoChangeStream");

                // Call the method under test
                resumeTokenService.saveResumeToken(resumeToken, "testThread");

                // Verify that the updateOne method is called with the correct arguments
                verify(resumeTokenCollection, times(1)).updateOne(
                                eq(Filters.eq("threadID", "testThread")),
                                eq(new Document("$set", expectedDocument)),
                                any(UpdateOptions.class));
        }

        @Test
        void testGetLatestResumeToken() {
                // Mock the FindIterable and expected document
                FindIterable<Document> findIterable = mock(FindIterable.class);
                Document latestTokenDoc = new Document("resumeToken", new Document("_data", "testData"))
                                .append("date", new Date());

                // Configure stubbing with thenReturn correctly
                when(resumeTokenCollection.find()).thenReturn(findIterable);
                when(findIterable.sort(any(Document.class))).thenReturn(findIterable);
                when(findIterable.first()).thenReturn(latestTokenDoc);

                // Call the method under test
                resumeTokenService.getLatestResumeToken();

                // Verify that the find method is called
                verify(resumeTokenCollection, times(1)).find();
                verify(findIterable, times(1)).sort(any(Document.class));
        }
}
