package com.example.demo;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.boot.test.context.SpringBootTest;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.FullDocument;

@SpringBootTest
class DemoApplicationTests {
	private static final String SANDS_DB = "test";
	private static final String PATRON = "patron";
	private static final String TABLE = "table";
	private static final String connectionString = "mongodb://ben:pass7word@localhost:27017";

	public static void main(String[] args) {
		final ServerApi serverApi = getServerApi();
		getMongSetting(serverApi);

		try (MongoClient mongoClient = getMongoClient()) {
			final MongoCollection<Document> patronColl = getCollection(mongoClient, PATRON);
			final MongoCollection<Document> tableColl = getCollection(mongoClient, TABLE);

			List<Bson> pipeline = Arrays.asList(Aggregates
					.match(Filters.in("operationType", Arrays.asList("insert", "update"))));

			final ChangeStreamIterable<Document> changeStream = patronColl.watch(pipeline)
					.fullDocument(FullDocument.UPDATE_LOOKUP);
			changeStream.forEach(event -> {
				System.out.println("Received a change to the collection: " + event);
			});
		}
	}

	private static MongoClientSettings getMongSetting(ServerApi serverApi) {
		MongoClientSettings settings = MongoClientSettings.builder()
				.applyConnectionString(new ConnectionString(connectionString)).serverApi(serverApi)
				.build();
		return settings;
	}

	private static ServerApi getServerApi() {
		ServerApi serverApi = ServerApi.builder().version(ServerApiVersion.V1).strict(true)
				.deprecationErrors(true).build();
		return serverApi;
	}

	private static MongoDatabase getDatabase(MongoClient mongoClient) {
		return mongoClient.getDatabase(SANDS_DB);
	}

	private static MongoClient getMongoClient() {
		final ServerApi serverApi = getServerApi();
		final MongoClientSettings mongoSetting = getMongSetting(serverApi);
		return MongoClients.create(mongoSetting);
	}

	private static MongoCollection<Document> getCollection(MongoClient mongoClient, String dbName) {
		return getDatabase(mongoClient).getCollection(dbName);
	}
}
