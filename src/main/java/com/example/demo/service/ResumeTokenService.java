package com.example.demo.service;

import java.util.Date;

import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

@Service
public class ResumeTokenService {
        private final Logger LOGGER = LoggerFactory.getLogger(ResumeTokenService.class);
        private final MongoCollection<Document> resumeTokenCollection;

        public ResumeTokenService(@Qualifier("resumeTokenCollection") MongoCollection<Document> resumeTokenCollection) {
                this.resumeTokenCollection = resumeTokenCollection;
        }

        public void saveResumeToken(BsonDocument resumeToken, String threadName) {
                Document mongoDocument = new Document().append("threadName", threadName)
                                .append("resumeToken", resumeToken).append("date", new Date())
                                .append("appName", "demoChangeStream");

                // Use upsert to ensure each thread only updates its own record
                resumeTokenCollection.updateOne(Filters.eq("threadID", threadName),
                                new Document("$set", mongoDocument), new UpdateOptions().upsert(true));

        }

        public BsonDocument getLatestResumeToken() {
                // Find the document with the latest date and retrieve its resume token
                // Get the latest token
                Document latestTokenDoc = resumeTokenCollection.find().sort(new Document("date", -1)).first();
                LOGGER.info("The latest resume token document: {}", latestTokenDoc);

                if (latestTokenDoc != null) {
                        // Retrieve the 'resumeToken' document from the retrieved MongoDB document
                        Document resumeTokenDoc = latestTokenDoc.get("resumeToken", Document.class);

                        if (resumeTokenDoc != null) {
                                // Extract the '_data' field from the 'resumeToken' as a string
                                String resumeTokenData = resumeTokenDoc.getString("_data");

                                if (resumeTokenData != null) {
                                        // Create a BsonDocument with the resume token data
                                        BsonDocument bsonResumeToken = BsonDocument
                                                        .parse("{\"_data\": \"" + resumeTokenData + "\"}");
                                        LOGGER.info("Found latest resume token: {}", bsonResumeToken);
                                        return bsonResumeToken;
                                }
                        }
                }

                // If no valid token is found, return null
                LOGGER.warn("No valid resume token found.");
                return null;
        }

}
