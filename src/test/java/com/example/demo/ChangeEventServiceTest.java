package com.example.demo;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.Date;
import java.util.List;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import com.example.demo.service.ChangeEventService;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

@SpringBootTest
class ChangeEventServiceTest {

        // Define the MongoCollection mocks with unique qualifiers to avoid duplicate
        // definition errors
        @MockBean(name = "changestreamCollection")
        private MongoCollection<Document> changestreamCollection;

        @MockBean(name = "userDailyTxnCollection")
        private MongoCollection<Document> userDailyTxnCollection;

        @MockBean
        private ChangeStreamIterable<Document> changeStreamIterable;

        @MockBean
        private FindIterable<Document> findIterable;

        @Autowired
        private ChangeEventService changeEventService;

        @Test
        void testChangeStreamIteratorWithResumeToken() {
                // Arrange
                BsonDocument resumeToken = BsonDocument.parse("{'_data': 'test'}");
                when(changestreamCollection.watch()).thenReturn(changeStreamIterable);
                when(changeStreamIterable.resumeAfter(resumeToken)).thenReturn(changeStreamIterable);

                // Act
                ChangeStreamIterable<Document> result = changeEventService.changeStreamIterator(resumeToken);

                // Assert
                verify(changestreamCollection, times(1)).watch();
                verify(changeStreamIterable, times(1)).resumeAfter(resumeToken);
                assertNotNull(result); // Ensuring result is not null
        }

        @Test
        void testChangeStreamIteratorWithoutResumeToken() {
                // Arrange
                when(changestreamCollection.watch()).thenReturn(changeStreamIterable);

                // Act
                ChangeStreamIterable<Document> result = changeEventService.changeStreamIterator(null);

                // Assert
                verify(changestreamCollection, times(1)).watch();
                verify(changeStreamIterable, never()).resumeAfter(any());
                assertNotNull(result); // Ensuring result is not null
        }

        @Test
        void testProcessChangeInsertsNewDocument() {
                // Arrange
                Document fullDocument = new Document("playerID", 123456789)
                                .append("transactionID", 100)
                                .append("value", 50.0)
                                .append("name", "ben")
                                .append("date", new Date());

                ChangeStreamDocument<Document> changeStreamDocument = mock(ChangeStreamDocument.class);
                when(changeStreamDocument.getFullDocument()).thenReturn(fullDocument);

                // Mocking the find operation to return no existing document
                when(userDailyTxnCollection.find(any(Document.class))).thenReturn(findIterable);
                when(findIterable.first()).thenReturn(null);

                // Act
                changeEventService.processChange(changeStreamDocument);

                // Assert: Verify updateOne with upsert
                verify(userDailyTxnCollection, times(1)).updateOne(
                                argThat(doc -> doc instanceof Document &&
                                                ((Document) doc).getInteger("playerID") == 123456789 &&
                                                ((Document) doc).containsKey("gamingDate")), // Match the filter
                                any(List.class), // Match any update pipeline
                                argThat(options -> options.isUpsert()) // Check that upsert is true
                );
        }

        @Test
        void testProcessChangeUpdatesExistingDocument() {
                // Arrange
                Document fullDocument = new Document("playerID", 123456789)
                                .append("transactionID", 101)
                                .append("name", "ben")
                                .append("value", 60.0)
                                .append("date", new Date());

                ChangeStreamDocument<Document> changeStreamDocument = mock(ChangeStreamDocument.class);
                when(changeStreamDocument.getFullDocument()).thenReturn(fullDocument);

                // Mocking the existing document
                Document existingDoc = new Document("playerID", 123456789)
                                .append("gamingDate", new Date())
                                .append("txns", List.of(new Document("transactionID", 101)
                                                .append("value", 60.0)
                                                .append("date", new Date())));

                when(userDailyTxnCollection.find(any(Document.class))).thenReturn(findIterable);
                when(findIterable.first()).thenReturn(existingDoc);

                // Act
                changeEventService.processChange(changeStreamDocument);

                // Assert: Verify updateOne with correct filter, update pipeline, and options
                verify(userDailyTxnCollection, times(1)).updateOne(
                                argThat(doc -> doc instanceof Document &&
                                                ((Document) doc).getInteger("playerID") == 123456789 &&
                                                ((Document) doc).containsKey("gamingDate")), // Match the filter
                                any(List.class), // Match any update pipeline
                                argThat(options -> options.isUpsert()) // Ensure that upsert is set to true
                );
        }

        @Test
        void testProcessChangeHandlesMissingFields() {
                // Arrange
                Document fullDocument = new Document(); // Missing expected fields
                ChangeStreamDocument<Document> changeStreamDocument = mock(ChangeStreamDocument.class);
                when(changeStreamDocument.getFullDocument()).thenReturn(fullDocument);

                // Act
                int result = changeEventService.processChange(changeStreamDocument);

                // Assert
                assertEquals(ChangeEventService.ERROR_INVALID_DOCUMENT, result); // Check the error code instead of
                                                                                 // exception
        }

}
