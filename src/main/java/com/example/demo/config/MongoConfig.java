package com.example.demo.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.demo.metrics.PrometheusMetricsConfig;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

@Configuration
public class MongoConfig {

    @Value("${spring.mongodb.uri}")
    private String mongoUri;

    @Value("${spring.mongodb.database}")
    private String databaseName;

    @Value("${spring.mongodb.collections}")
    private String[] collections;

    @Value("${spring.mongodb.resumetoken.collection}")
    private String resumeTokenCollectionName;

    // Create a SINGLE MongoClient bean (Reuse this everywhere)
    @Bean
    public MongoClient mongoClient() {
        return MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(mongoUri))
                .applyToConnectionPoolSettings(builder -> builder.maxSize(128).minSize(64))
                .applyToSocketSettings(builder -> builder.connectTimeout(30, TimeUnit.SECONDS))
                .retryWrites(true)
                .readPreference(ReadPreference.nearest())
                .writeConcern(WriteConcern.MAJORITY)
                .applicationName("changeStreamDemo")
                .build());
    }

    @Bean
    public MongoDatabase mongoDatabase(MongoClient mongoClient) {
        return mongoClient.getDatabase(databaseName);
    }

    @Bean
    public Map<String, MongoCollection<Document>> collectionMap(MongoDatabase mongoDatabase) {
        Map<String, MongoCollection<Document>> collectionMap = new HashMap<>();

        if (collections == null || collections.length == 0) {
            throw new IllegalStateException("ERROR: No collections defined in 'spring.mongodb.collections'.");
        }

        System.out.printf("✅ Registering collections: %s \n", Arrays.toString(collections));
        
        for (String collectionName : collections) {
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
            collectionMap.put(collectionName, collection);
            System.out.println("✅ Registered collection: in map " + collectionName);
        }

         // ✅ Explicitly add the resume token collection to avoid overwriting
         collectionMap.put(resumeTokenCollectionName, mongoDatabase.getCollection(resumeTokenCollectionName));
         System.out.println("✅ Registered collection: " + resumeTokenCollectionName);
 
        return collectionMap;
    }

    /*
    @Bean
    public MongoCollection<Document> resumeTokenCollection(MongoDatabase mongoDatabase) {
        return mongoDatabase.getCollection(resumeTokenCollectionName);
    }
    */

    /** ✅ Register Prometheus Metrics for Each Collection from Config */
    @PostConstruct
    public void registerPrometheusMetrics() {
        for (String collection : collections) {
            PrometheusMetricsConfig.getInstance(collection);
        }
        // Also register for resume tokens collection
        PrometheusMetricsConfig.getInstance(resumeTokenCollectionName);
    }
}
