package com.example.demo;

import org.bson.Document;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import com.example.demo.service.ChangeEventService;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

class ChangeEventServiceTest {

        // Mock the MongoCollection that is injected into ChangeEventService
        @Mock
        private MongoCollection<Document> changestreamCollection;

        // Mock the FindIterable to simulate the MongoDB find operation
        @Mock
        private FindIterable<Document> mockFindIterable;

        // Mock ChangeStreamDocument to simulate change events
        @Mock
        private ChangeStreamDocument<Document> mockEvent;

        // Inject mocks into ChangeEventService
        @InjectMocks
        private ChangeEventService changeEventService;

        @BeforeEach
        void setUp() {
                // Initialize mocks and inject them into the service
                MockitoAnnotations.openMocks(this);

                // Set up mock behavior for changestreamCollection
                when(changestreamCollection.find()).thenReturn(mockFindIterable);
                when(mockFindIterable.sort(any(Document.class))).thenReturn(mockFindIterable);
                when(mockFindIterable.first()).thenReturn(new Document("key", "value")); // Mocked response
        }

        @Test
        void testProcessChange() {
                // Set up the mock ChangeStreamDocument
                Document document = new Document("key", "value");
                when(mockEvent.getFullDocument()).thenReturn(document);

                // Call the processChange method with the mock object
                int result = changeEventService.processChange(mockEvent, 1);

                // Verify the result is as expected
                assertEquals(0, result, "The processChange method should return 0 as expected");

                // Verify that the find operation on changestreamCollection was called
                verify(changestreamCollection, times(1)).find();
                verify(mockFindIterable, times(1)).sort(any(Document.class));
                verify(mockFindIterable, times(1)).first();
        }
}
