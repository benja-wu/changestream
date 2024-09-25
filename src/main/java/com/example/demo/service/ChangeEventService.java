package com.example.demo.service;

import java.util.Date;
import java.util.List;

import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * Customized business logic for handling one change stream event
 */
@Service
public class ChangeEventService {

        private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventService.class);
        private final MongoCollection<Document> changestreamCollection;
        private final MongoCollection<Document> userDailyTxnCollection;

        public ChangeEventService(
                        @Qualifier("changestreamCollection") MongoCollection<Document> changestreamCollection,
                        @Qualifier("userDailyTxnCollection") MongoCollection<Document> userDailyTxnCollection) {
                this.changestreamCollection = changestreamCollection;
                this.userDailyTxnCollection = userDailyTxnCollection;
        }

        public ChangeStreamIterable<Document> changeStreamIterator(BsonDocument resumeToken) {
                // Start the change stream with or without a resume token
                return resumeToken != null
                                ? changestreamCollection.watch().resumeAfter(resumeToken)
                                : changestreamCollection.watch();
        }

        /**
         * Use the event's userID find document in userDailyTnx collection
         *
         * @param event
         * @return
         */
        public int processChange(ChangeStreamDocument<Document> event) {
                Document fullDocument = event.getFullDocument();

                // add one logic about whether we need to process this event or not

                // Extract necessary fields from the event
                int userID = fullDocument.getInteger("userID");
                int transactionID = fullDocument.getInteger("transactionID");
                double value = fullDocument.getDouble("value");
                Date date = fullDocument.getDate("date");

                // Construct the transaction object
                Document transaction = new Document("transactionID", transactionID)
                                .append("value", value)
                                .append("date", date);

                // Define filter to find user document by userID
                Document filter = new Document("userID", userID);

                // Update operation: find the document and update the transaction array
                Document update = new Document("$set", new Document("lastModified", new Date()))
                                .append("$addToSet", new Document("txns", transaction));

                // Check if the document exists
                Document existingDoc = userDailyTxnCollection.find(filter).first();

                if (existingDoc == null) {
                        // Document doesn't exist, insert a new one
                        Document newUserDoc = new Document("userID", userID)
                                        .append("txns", List.of(transaction))
                                        .append("lastModified", new Date());

                        userDailyTxnCollection.insertOne(newUserDoc);
                        LOGGER.info("Inserted new document for userID: {}", userID);
                } else {
                        // Document exists, update the txns field
                        userDailyTxnCollection.updateOne(filter, update, new UpdateOptions().upsert(true));
                        LOGGER.info("Updated document for userID: {} with transactionID: {}", userID, transactionID);
                }

                return 0;
        }
}
