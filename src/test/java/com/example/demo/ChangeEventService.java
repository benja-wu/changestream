package com.example.demo;

import static org.junit.jupiter.api.Assertions.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.example.demo.service.ChangeEventService;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

class ChangeEventServiceTest {

        // Create an instance of ChangeEventService
        private final ChangeEventService changeEventService = new ChangeEventService();

        @Test
        void testProcessChange() {
                // Create a mock of ChangeStreamDocument using Mockito
                ChangeStreamDocument<Document> mockEvent = Mockito.mock(ChangeStreamDocument.class);

                // Optionally, you can set up mock behavior if needed
                Document document = new Document();
                Mockito.when(mockEvent.getFullDocument()).thenReturn(document);

                // Call the processChange method with the mock object
                int result = changeEventService.processChange(mockEvent, 1);

                // Verify that the method returns the expected value
                assertEquals(0, result); // This assertion checks the method's return value
        }
}
