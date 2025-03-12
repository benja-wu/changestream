package com.example.demo.service.impl;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.example.demo.metrics.PrometheusMetricsConfig;
import com.example.demo.metrics.TpsCalculator;
import com.example.demo.service.BusinessTask;
import com.example.demo.service.ResumeTokenService;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

@Service
public class Task1 extends BusinessTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(Task1.class);

    // the watching target collection name in MongoDB
    private static final String TASK_COLLECTION_NAME = "changestream";

    public Task1(ResumeTokenService resumeTokenService, TpsCalculator tpsCalculator, MongoClient mongoClient) {
        // ✅ Manually retrieve PrometheusMetricsConfig
        super(resumeTokenService, tpsCalculator, 
              PrometheusMetricsConfig.getInstance(TASK_COLLECTION_NAME), // ✅ No @Autowired
              TASK_COLLECTION_NAME, mongoClient);
    }


    @Override
    protected int processChange(ChangeStreamDocument<Document> event) {
        Document fullDocument = event.getFullDocument();
        if (fullDocument == null || !fullDocument.containsKey("playerID") || 
            !fullDocument.containsKey("transactionID") || !fullDocument.containsKey("value") ||
            !fullDocument.containsKey("name") || !fullDocument.containsKey("date")) {
            LOGGER.error("Invalid document: Missing required fields, doc {}", fullDocument);
            return -1;
        }

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

        LOGGER.info("Processing event for playerID: {}, gamingDate: {}", playerID, gamingDate);

        Document filter = new Document("playerID", playerID).append("gamingDate", gamingDate);
        Document newTransaction = new Document("transactionID", transactionID)
                .append("value", value)
                .append("date", date);

        Document updateTxns = new Document("$let", new Document("vars", 
            new Document("existingTxn", new Document("$first", new Document("$filter", 
                new Document("input", "$txns").append("cond", 
                new Document("$eq", List.of("$$this.transactionID", transactionID)))))))
            .append("in", new Document("$cond", new Document("if", 
                new Document("$not", List.of("$$existingTxn")))
                .append("then", new Document("$concatArrays", List.of(
                    new Document("$ifNull", List.of("$txns", List.of())), List.of(newTransaction))))
                .append("else", new Document("$map", new Document("input", "$txns")
                    .append("as", "txn")
                    .append("in", new Document("$cond", new Document("if", 
                        new Document("$eq", List.of("$$txn.transactionID", transactionID)))
                        .append("then", newTransaction)
                        .append("else", "$$txn"))))))));

        Document setOperation = new Document("$set", new Document()
            .append("playerID", playerID)
            .append("gamingDate", gamingDate)
            .append("name", new Document("$ifNull", List.of("$name", name)))
            .append("txns", updateTxns)
            .append("lastModified", new Date()));

        List<Document> updatePipeline = List.of(setOperation);
        UpdateOptions options = new UpdateOptions().upsert(true);
        
        collection.updateOne(filter, updatePipeline, options);

        LOGGER.info("Processed update for playerID: {} and transactionID: {}", playerID, transactionID);
        return 0;
    }
}