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
import com.mongodb.client.model.UpdateOptions;
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
        }

        @Test
        void testProcessChangeInsertsNewDocument() {
                // Arrange
                Document fullDocument = new Document("userID", 1)
                                .append("transactionID", 100)
                                .append("value", 50.0)
                                .append("date", new Date());

                ChangeStreamDocument<Document> changeStreamDocument = mock(ChangeStreamDocument.class);
                when(changeStreamDocument.getFullDocument()).thenReturn(fullDocument);
                when(userDailyTxnCollection.find(any(Document.class))).thenReturn(findIterable);
                when(findIterable.first()).thenReturn(null);

                // Act
                changeEventService.processChange(changeStreamDocument);

                // Assert
                verify(userDailyTxnCollection, times(1)).insertOne(any(Document.class));
        }

        @Test
        void testProcessChangeUpdatesExistingDocument() {
                // Arrange
                Document fullDocument = new Document("userID", 1)
                                .append("transactionID", 101)
                                .append("value", 60.0)
                                .append("date", new Date());

                ChangeStreamDocument<Document> changeStreamDocument = mock(ChangeStreamDocument.class);
                when(changeStreamDocument.getFullDocument()).thenReturn(fullDocument);

                Document existingDoc = new Document("userID", 1).append("txns", List.of());
                when(userDailyTxnCollection.find(any(Document.class))).thenReturn(findIterable);
                when(findIterable.first()).thenReturn(existingDoc);

                // Act
                changeEventService.processChange(changeStreamDocument);

                // Assert
                verify(userDailyTxnCollection, times(1)).updateOne(
                                eq(new Document("userID", 1)),
                                any(Document.class),
                                any(UpdateOptions.class));
        }

        @Test
        void testProcessChangeHandlesMissingFields() {
                // Arrange
                Document fullDocument = new Document(); // Missing expected fields
                ChangeStreamDocument<Document> changeStreamDocument = mock(ChangeStreamDocument.class);
                when(changeStreamDocument.getFullDocument()).thenReturn(fullDocument);

                // Act & Assert
                assertThrows(NullPointerException.class, () -> changeEventService.processChange(changeStreamDocument));
        }
}
