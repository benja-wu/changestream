package com.example.demo.service;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

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
public class ChangeEventService implements ChangeEventServiceInterface {

        private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventService.class);
        private final MongoCollection<Document> changestreamCollection;
        private final MongoCollection<Document> userDailyTxnCollection;
        public static final int ERROR_INVALID_DOCUMENT = -1; // Error code for invalid documents
        public static final int ERROR_BUSINESS_LOGIC = -2; // Error code for invalid documents

        public ChangeEventService(
                        @Qualifier("changestreamCollection") MongoCollection<Document> changestreamCollection,
                        @Qualifier("userDailyTxnCollection") MongoCollection<Document> userDailyTxnCollection) {
                this.changestreamCollection = changestreamCollection;
                this.userDailyTxnCollection = userDailyTxnCollection;
        }

        @Override
        public ChangeStreamIterable<Document> changeStreamIterator(BsonDocument resumeToken) {
                // Start the change stream with or without a resume token
                return resumeToken != null
                                ? changestreamCollection.watch().resumeAfter(resumeToken)
                                : changestreamCollection.watch();
        }

        /**
         * Use the event's playerID and gameDate to find document in userDailyTnx
         * collection
         *
         * @param event
         * @return
         */
        public int processChangeMultipleCommands(ChangeStreamDocument<Document> event) {
                Document fullDocument = event.getFullDocument();
                // Validate necessary fields from the event
                if (!fullDocument.containsKey("playerID") ||
                                !fullDocument.containsKey("transactionID") ||
                                !fullDocument.containsKey("value") ||
                                !fullDocument.containsKey("name") ||
                                !fullDocument.containsKey("date")) {
                        LOGGER.error("Invalid document: Missing required fields, doc {}", fullDocument);
                        return ERROR_INVALID_DOCUMENT; // Return error code for missing fields
                }

                // Extract necessary fields from the event
                int playerID = fullDocument.getInteger("playerID");
                int transactionID = fullDocument.getInteger("transactionID");
                double value = fullDocument.getDouble("value");
                Date date = fullDocument.getDate("date");
                String name = fullDocument.getString("name");
                // Convert the date to midnight UTC
                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                calendar.setTime(date);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);

                // Set the modified date to midnight UTC
                Date gamingDate = calendar.getTime();

                LOGGER.info("process event data: {} ", gamingDate);

                // Construct the transaction object
                Document newTransaction = new Document("transactionID", transactionID)
                                .append("value", value)
                                .append("date", date);

                // Define the filter to find the document using playerID and gamingDate
                Document filter = new Document("playerID", playerID)
                                .append("gamingDate", gamingDate);
                // Check if the document exists
                Document existingDoc = userDailyTxnCollection.find(filter).first();

                if (existingDoc == null) {
                        // Document doesn't exist, insert a new one with the new transaction
                        Document newUserDoc = new Document("playerID", playerID)
                                        .append("gamingDate", gamingDate)
                                        .append("name", name)
                                        .append("txns", List.of(newTransaction))
                                        .append("lastModified", new Date());

                        userDailyTxnCollection.insertOne(newUserDoc);
                        LOGGER.info("Inserted new document for playerID: {} and gamingDate: {}", playerID,
                                        gamingDate);
                } else {
                        // Check if the transaction with the given transactionID exists
                        boolean transactionExists = existingDoc.getList("txns", Document.class).stream()
                                        .anyMatch(txn -> txn.getInteger("transactionID") == transactionID);

                        if (transactionExists) {
                                // Update only the existing transaction within the txns array
                                Document updateTransaction = new Document("$set",
                                                new Document("txns.$[elem]", newTransaction));
                                UpdateOptions updateOptions = new UpdateOptions().arrayFilters(
                                                List.of(new Document("elem.transactionID", transactionID)));

                                // Update the transaction element inside the array
                                userDailyTxnCollection.updateOne(filter, updateTransaction, updateOptions);
                                LOGGER.info("Updated existing transaction for playerID: {} and transactionID: {}",
                                                playerID, transactionID);

                                // Update the lastModified field separately without array filters
                                Document updateLastModified = new Document("$set",
                                                new Document("lastModified", new Date()));
                                userDailyTxnCollection.updateOne(filter, updateLastModified);
                        } else {
                                // Append the new transaction to the txns array and update lastModified
                                Document update = new Document("$push", new Document("txns", newTransaction))
                                                .append("$set", new Document("lastModified", new Date()));

                                userDailyTxnCollection.updateOne(filter, update);
                                LOGGER.info("Appended new transaction for playerID: {} and transactionID: {}",
                                                playerID, transactionID);
                        }
                }

                return 0;
        }

        /**
         * Handle the upsert doc, replacing/push transaction into txns array field with
         * one update command. process change is idempotent, so after resumeing, it can 
         * handle already-handled event correctly. 
         * 
         * @param event
         * @return
         */
        @Override
        public int processChange(ChangeStreamDocument<Document> event) {
                Document fullDocument = event.getFullDocument();
                // Validate necessary fields from the event
                if (!fullDocument.containsKey("playerID") ||
                                !fullDocument.containsKey("transactionID") ||
                                !fullDocument.containsKey("value") ||
                                !fullDocument.containsKey("name") ||
                                !fullDocument.containsKey("date")) {
                        LOGGER.error("Invalid document: Missing required fields, doc {}", fullDocument);
                        return ERROR_INVALID_DOCUMENT; // Return error code for missing fields
                }

                // Extract necessary fields from the event
                int playerID = fullDocument.getInteger("playerID");
                int transactionID = fullDocument.getInteger("transactionID");
                double value = fullDocument.getDouble("value");
                Date date = fullDocument.getDate("date");
                String name = fullDocument.getString("name");

                // Convert the date to midnight UTC
                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                calendar.setTime(date);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                Date gamingDate = calendar.getTime();

                LOGGER.info("process event data: {}", gamingDate);

                // Define the filter to find the document using playerID and gamingDate
                Document filter = new Document("playerID", playerID)
                                .append("gamingDate", gamingDate);

                // Define the transaction object
                Document newTransaction = new Document("transactionID", transactionID)
                                .append("value", value)
                                .append("date", date);

                // Define the conditions and operations used in the update pipeline
                Document setNameIfNull = new Document("$ifNull", List.of("$name", name));
                Document existingTransactionCondition = new Document("$eq",
                                List.of("$$this.transactionID", transactionID));
                Document filterExistingTransaction = new Document("$filter", new Document("input", "$txns")
                                .append("cond", existingTransactionCondition));
                Document firstExistingTransaction = new Document("$first", filterExistingTransaction);

                // Define the replacement or append logic
                Document appendNewTransaction = new Document("$concatArrays", List.of(
                                new Document("$ifNull", List.of("$txns", List.of())), List.of(newTransaction)));

                Document mapTransactions = new Document("$map", new Document("input", "$txns")
                                .append("as", "txn")
                                .append("in", new Document("$cond", new Document("if",
                                                new Document("$eq", List.of("$$txn.transactionID", transactionID)))
                                                .append("then", newTransaction)
                                                .append("else", "$$txn"))));

                Document replaceOrAppendTransaction = new Document("$cond",
                                new Document("if", new Document("$not", List.of("$$existingTxn")))
                                                .append("then", appendNewTransaction)
                                                .append("else", mapTransactions));

                // Assemble the $let and $set operations in the update pipeline
                Document letOperation = new Document("$let",
                                new Document("vars", new Document("existingTxn", firstExistingTransaction))
                                                .append("in", replaceOrAppendTransaction));

                Document setOperation = new Document("$set", new Document()
                                .append("playerID", playerID)
                                .append("gamingDate", gamingDate)
                                .append("name", setNameIfNull)
                                .append("txns", letOperation)
                                .append("lastModified", new Date()));

                // Define the update pipeline
                List<Document> updatePipeline = List.of(setOperation);

                // Perform the update operation with upsert true
                UpdateOptions options = new UpdateOptions().upsert(true);
                userDailyTxnCollection.updateOne(filter, updatePipeline, options);

                LOGGER.info("Processed update for playerID: {} and transactionID: {}", playerID, transactionID);
                return 0;
        }

}
