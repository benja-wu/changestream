package com.example.demo.service;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

@Service
public class ChangeEventService {

        private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventService.class);
        private final MongoCollection<Document> changestreamCollection;

        public ChangeEventService(
                        @Qualifier("changestreamCollection") MongoCollection<Document> changestreamCollection) {
                this.changestreamCollection = changestreamCollection;
        }

        // business logic
        public int processChange(ChangeStreamDocument<Document> event, int threadId) {
                // SJM business logic
                // Create a Random object for generating random sleep times
                Document latestEventDoc = changestreamCollection.find().sort(new Document("date", -1)).first();
                LOGGER.info("The latest event document: {}", latestEventDoc);
                return 0; // Return appropriate value based on your logic
        }

}
