package com.example.demo.service.impl;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.example.demo.metrics.PrometheusMetricsConfig;
import com.example.demo.metrics.TpsCalculator;
import com.example.demo.service.BusinessTask;
import com.example.demo.service.ResumeTokenService;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

@Service
public class Task2 extends BusinessTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(Task2.class);
    
    // the watching target collection name in MongoDBO
    private static final String TASK_COLLECTION_NAME = "userdailytxn";

    public Task2(ResumeTokenService resumeTokenService, TpsCalculator tpsCalculator, MongoClient mongoClient) {
        // ✅ Manually retrieve the PrometheusMetricsConfig instance
        super(resumeTokenService, tpsCalculator, 
              PrometheusMetricsConfig.getInstance(TASK_COLLECTION_NAME), // ✅ No @Autowired
              TASK_COLLECTION_NAME, mongoClient);
    }

    @Override
    protected int processChange(ChangeStreamDocument<Document> event) {
        Document fullDocument = event.getFullDocument();
        LOGGER.info("Processing document in Task2: {}", fullDocument);
        return 0;
    }
}
