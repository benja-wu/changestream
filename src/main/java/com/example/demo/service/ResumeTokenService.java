package com.example.demo.service;

import java.util.Map;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

/**
 * ResumeTokenService helps to store and fetch the resume token for each change stream.
 * Uses MongoDB as a persistent resume token store.
 */
@Service
public class ResumeTokenService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResumeTokenService.class);
    private final Map<String, MongoCollection<Document>> collectionMap;
    private final String resumeTokenCollectionName;  // ✅ Now passed via constructor

    /**
     * Constructor injection ensures `resumeTokenCollectionName` is set before use.
     */
    public ResumeTokenService(Map<String, MongoCollection<Document>> collectionMap, 
                              @org.springframework.beans.factory.annotation.Value("${spring.mongodb.resumetoken.collection}") 
                              String resumeTokenCollectionName) {
        this.collectionMap = collectionMap;
        this.resumeTokenCollectionName = resumeTokenCollectionName;
    }

    /**
     * Saves the resume token for a specific collection and thread.
     */
    public void saveResumeToken(BsonTimestamp bsonTimestamp, BsonDocument resumeToken, 
                                String threadName, String collectionName) {
        MongoCollection<Document> resumeTokenCollection = getResumeTokenCollection();
        if (resumeTokenCollection == null) {
            LOGGER.error("❌ Resume token collection not found for: {}", collectionName);
            return;
        }
    
        Document mongoDocument = new Document()
                .append("collectionName", collectionName)  // ✅ Store per collection
                .append("threadName", threadName)
                .append("resumeToken", resumeToken)
                .append("date", bsonTimestamp)
                .append("appName", "demoChangeStream");

        resumeTokenCollection.updateOne(
                Filters.and(Filters.eq("collectionName", collectionName), Filters.eq("threadName", threadName)),
                new Document("$set", mongoDocument),
                new UpdateOptions().upsert(true)
        );

        LOGGER.info("✅ Saved resume token for collection: {} | thread: {}", collectionName, threadName);
    }

    /**
     * Retrieves the latest resume token for a specific collection.
     */
    public BsonDocument getResumeToken(String collectionName) {
        LOGGER.info("🔍 in get resume token func");
        MongoCollection<Document> resumeTokenCollection = getResumeTokenCollection();
        if (resumeTokenCollection == null) {
            LOGGER.error("❌ Resume token collection not found in map!");
            return null;
        }

        Document latestTokenDoc = resumeTokenCollection
                .find(Filters.eq("collectionName", collectionName))  // ✅ Retrieve only for the collection
                .sort(new Document("date", 1))  // Get the earliest recorded token
                .first();

        LOGGER.info("🔍 Latest resume token document for {}: {}", collectionName, latestTokenDoc);

        if (latestTokenDoc != null) {
            Document resumeTokenDoc = latestTokenDoc.get("resumeToken", Document.class);
            if (resumeTokenDoc != null) {
                String resumeTokenData = resumeTokenDoc.getString("_data");
                if (resumeTokenData != null) {
                    BsonDocument bsonResumeToken = BsonDocument.parse("{\"_data\": \"" + resumeTokenData + "\"}");
                    LOGGER.info("✅ Found resume token for {}: {}", collectionName, bsonResumeToken);
                    return bsonResumeToken;
                }
            }
        }

        LOGGER.warn("⚠️ No valid resume token found for collection: {}", collectionName);
        return null;
    }

    /**
     * Helper method to retrieve the correct resume token collection from `collectionMap`.
     */
    private MongoCollection<Document> getResumeTokenCollection() {
        LOGGER.info("Info collection for resume token: {}, map is {}", resumeTokenCollectionName, collectionMap); 
        MongoCollection<Document> doc = collectionMap.get(resumeTokenCollectionName);
        if (doc == null) {
            LOGGER.error("❌ Resume token collection '{}' not found in map!", resumeTokenCollectionName);
        } else {
            LOGGER.info("✅ Found collection for resume token: {}, map is {}", resumeTokenCollectionName, collectionMap);
        }
        return doc;
    }
}
