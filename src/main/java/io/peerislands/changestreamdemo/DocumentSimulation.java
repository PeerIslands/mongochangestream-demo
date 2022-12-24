package io.peerislands.changestreamdemo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.text.DecimalFormat;

import static java.util.concurrent.ThreadLocalRandom.current;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;


public class DocumentSimulation {

    MongoCollection<Document> collection = null;

    public DocumentSimulation() {

        this.initDB();
        this.simulateDocs();
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

    private double getRandomDouble() {
        double nextDouble = current().nextDouble(50,100);
        DecimalFormat df = new DecimalFormat("###.##");
        return Double.parseDouble(df.format(nextDouble));
    }

    private void simulateDocs() {

        while(true) {
            Document document = new Document();
            document.put("Symbol", "MDB");
            document.put("Price", getRandomDouble());
            collection.insertOne(document);
            System.out.println(document.toJson());
            try {
                Thread.sleep(2000L + (long) (1000 * Math.random()));
            }
            catch(Exception e) {
                System.err.println(e);
            }
        }
    }

    public static void main(String[] args) {
        new DocumentSimulation();
    }
}
