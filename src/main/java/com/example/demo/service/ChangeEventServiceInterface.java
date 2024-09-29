// ChangeEventServiceInterface.java
package com.example.demo.service;

import org.bson.BsonDocument;
import org.bson.Document;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * ChangeEventServiceInterface abstract the user focused MongoDB changestream
 * initialization and
 * actual handling behaviors.
 */
public interface ChangeEventServiceInterface {
        /**
         * Return one change stream iterator by provided resumeToken
         * 
         * @param resumeToken
         * @return
         */
        ChangeStreamIterable<Document> changeStreamIterator(BsonDocument resumeToken);

        /**
         * Customized business logic for handling one MongoDB change stream event
         * 
         * @param event
         * @return
         */
        int processChange(ChangeStreamDocument<Document> event);
}