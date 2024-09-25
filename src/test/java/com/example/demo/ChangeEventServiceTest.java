// ChangeEventServiceTest.java
package com.example.demo;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.example.demo.service.ChangeEventService;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;

class ChangeEventServiceTest {

        @Mock
        private MongoCollection<Document> changestreamCollection;

        @InjectMocks
        private ChangeEventService changeEventService;

        @BeforeEach
        void setUp() {
                MockitoAnnotations.openMocks(this);
        }

        @Test
        void testChangeStreamIteratorWithResumeToken() {
                BsonDocument resumeToken = BsonDocument.parse("{'_data': 'test'}");
                ChangeStreamIterable<Document> changeStreamIterable = mock(ChangeStreamIterable.class);

                when(changestreamCollection.watch()).thenReturn(changeStreamIterable);
                when(changeStreamIterable.resumeAfter(resumeToken)).thenReturn(changeStreamIterable);

                changeEventService.changeStreamIterator(resumeToken);

                verify(changestreamCollection, times(1)).watch();
                verify(changeStreamIterable, times(1)).resumeAfter(resumeToken);
        }

        @Test
        void testChangeStreamIteratorWithoutResumeToken() {
                ChangeStreamIterable<Document> changeStreamIterable = mock(ChangeStreamIterable.class);

                when(changestreamCollection.watch()).thenReturn(changeStreamIterable);

                changeEventService.changeStreamIterator(null);

                verify(changestreamCollection, times(1)).watch();
                verify(changeStreamIterable, never()).resumeAfter(any());
        }
}