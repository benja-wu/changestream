package com.example.demo.config;

import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.connection.ConnectionPoolSettings;

@Configuration
public class MongoConfig {

        @Value("${spring.mongodb.uri}")
        private String mongoUri;

        @Value("${spring.mongodb.resumetoken.collection}")
        private String resumeTokenCollName;

        @Value("${spring.mongodb.collection}")
        private String collName;

        @Value("${spring.mongodb.txn.collection}")
        private String txncollName;

        @Value("${spring.mongodb.database}")
        private String dbName;

        // Bean configuration for MongoClient
        @Bean
        public MongoClient mongoClient() {
                MongoClientSettings clientSettings = MongoClientSettings.builder()
                                .applyConnectionString(new ConnectionString(mongoUri))
                                .applyToConnectionPoolSettings((ConnectionPoolSettings.Builder builder) -> builder
                                                .maxSize(128).minSize(64))
                                .applyToSocketSettings(builder -> builder.connectTimeout(30, TimeUnit.SECONDS))
                                .retryWrites(true).readPreference(ReadPreference.nearest())
                                .writeConcern(WriteConcern.MAJORITY).applicationName("changeStreamDemo").build();

                return MongoClients.create(clientSettings);
        }

        // Bean configuration for the Resume Token Collection
        @Bean
        public MongoCollection<Document> resumeTokenCollection(MongoClient mongoClient) {
                return mongoClient.getDatabase(dbName).getCollection(resumeTokenCollName, Document.class);
        }

        // Bean configuration for the main collection that DemoApplication watches
        @Bean
        public MongoCollection<Document> changestreamCollection(MongoClient mongoClient) {
                return mongoClient.getDatabase(dbName).getCollection(collName, Document.class);
        }

        @Bean
        public MongoCollection<Document> userDailyTxnCollection(MongoClient mongoClient) {
                return mongoClient.getDatabase(dbName).getCollection(txncollName, Document.class);
        }
}
