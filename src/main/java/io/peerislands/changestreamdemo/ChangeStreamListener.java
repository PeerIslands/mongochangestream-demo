package io.peerislands.changestreamdemo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.function.Consumer;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.changestream.FullDocument.UPDATE_LOOKUP;
import static java.util.Arrays.asList;
import static com.mongodb.client.model.Filters.*;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class ChangeStreamListener {

    MongoCollection<Document> collection = null;
    BsonDocument resumeToken = null;

    public ChangeStreamListener() {
        initDB();
        simpleListen();
    }

    private void initDB() {
        ConnectionString connectionString = new ConnectionString(Constants.DB_URI);
        CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
        CodecRegistry codecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), pojoCodecRegistry);

        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .codecRegistry(codecRegistry)
                .build();
        MongoClient mongoClient = MongoClients.create(clientSettings);
        MongoDatabase db = mongoClient.getDatabase(Constants.DB);
        collection = db.getCollection(Constants.COLLECTION, Document.class);
    }

    private void simpleListen() {
        ChangeStreamIterable<Document> changes = collection.watch();
        changes.forEach(printEvent());
    }

    private void filterListener() {
        List<Bson> pipeline = singletonList(match(in("operationType", asList("insert", "delete"))));
        ChangeStreamIterable<Document> changes = collection.watch(pipeline);
        changes.forEach(printEvent());
    }

    private void updateListener() {
        List<Bson> pipeline = singletonList(match(eq("operationType", "update")));
        ChangeStreamIterable<Document> changes = collection.watch(pipeline).fullDocument(UPDATE_LOOKUP);
        changes.forEach(printEvent());
    }

    private void filterListenerWithResumeToken() {
        List<Bson> pipeline = singletonList(match(in("operationType", asList("insert", "delete"))));

        if(resumeToken != null) {
            ChangeStreamIterable<Document> changes = collection.watch(pipeline).resumeAfter(resumeToken);
            changes.forEach(printEvent());
        }else {
            ChangeStreamIterable<Document> changes = collection.watch(pipeline);
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changes.cursor();
            System.out.println("==> Going through the stream a first time & record a resumeToken");
            int indexOfOperationToRestartFrom = 5;
            int indexOfIncident = 8;
            int counter = 0;
            while (cursor.hasNext() && counter != indexOfIncident) {
                ChangeStreamDocument<Document> event = cursor.next();
                if (indexOfOperationToRestartFrom == counter) {
                    resumeToken = event.getResumeToken();
                }
                System.out.println(event);
                counter++;
            }
            changes.forEach(printEvent());
        }
    }

    private static Consumer<ChangeStreamDocument<Document>> printEvent() {
        return System.out::println;
    }

    public static void main(String[] args) {
        new ChangeStreamListener();
    }
}
