package com.example.demo.service;

import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * Customized business logic for handling one change stream event
 */
@Service
public class ChangeEventService {

        private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventService.class);
        private final MongoCollection<Document> changestreamCollection;

        public ChangeEventService(
                        @Qualifier("changestreamCollection") MongoCollection<Document> changestreamCollection) {
                this.changestreamCollection = changestreamCollection;
        }

        public ChangeStreamIterable<Document> changeStreamIterator(BsonDocument resumeToken) {
                // Start the change stream with or without a resume token
                ChangeStreamIterable<Document> changeStream = resumeToken != null
                                ? changestreamCollection.watch().resumeAfter(resumeToken)
                                : changestreamCollection.watch();

                return changeStream;

        }

        // business logic
        public int processChange(ChangeStreamDocument<Document> event) {
                // SJM business logic
                // Create a Random object for generating random sleep times
                Document latestEventDoc = changestreamCollection.find().sort(new Document("date", -1)).first();
                LOGGER.info("The latest event document: {}", latestEventDoc);
                return 0; // Return appropriate value based on your logic
        }

}
