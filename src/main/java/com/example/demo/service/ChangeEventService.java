package com.example.demo.service;

import java.util.Random;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

@Service
public class ChangeEventService {

        private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventService.class);

        // business logic
        public int processChange(ChangeStreamDocument<Document> event, int threadId) {
                // SJM business logic
                // Create a Random object for generating random sleep times
                Random random = new Random();

                // Generate a random sleep time between 10 ms and 200 ms
                int sleepTime = 10 + random.nextInt(100); // 10 to 200 ms

                try {
                        // Simulate processing time by sleeping
                        Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // Restore interrupted status
                        LOGGER.error("Thread was interrupted while sleeping", e);
                }

                // Simulated business logic processing (you can add actual logic here)
                LOGGER.info("Processed event: {} with simulated sleep time: {} ms", event, sleepTime);
                return 0; // Return appropriate value based on your logic
        }

}
