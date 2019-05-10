package org.aksw.simba.tapioca.preprocessing.labelretrieving;

import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;

public class MongoDBBasedTokenizedLabelRetriever extends AbstractTokenizedLabelRetriever implements AutoCloseable {
    
    public static final String DB_NAME = "tapioca";
    public static final String COLLECTION_URIS = "uris";
    public static final String URI_FIELD = "uri";
    public static final String TOKENS_FIELD = "tokens";
    
    protected MongoClient client;
    protected MongoCollection<Document> uriCollection;

    public MongoDBBasedTokenizedLabelRetriever(MongoClient client) {
        this.client = client;
        uriCollection = open(client);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String> getTokenizedLabel(String uri, String namespace) {
        Document query = new Document(URI_FIELD, uri);
        Iterator<Document> iterator = uriCollection.find(query).iterator();
        if (iterator.hasNext()) {
            return (List<String>) iterator.next().get(TOKENS_FIELD);
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    public static MongoDBBasedTokenizedLabelRetriever create(String dbHost, int dbPort) {
        return new MongoDBBasedTokenizedLabelRetriever(new MongoClient(dbHost, dbPort));
    }

    public static MongoCollection<Document> open(MongoClient client) {
        MongoDatabase mongoDB = client.getDatabase(DB_NAME);
        if (!queueTableExists(mongoDB)) {
            return initDB(mongoDB);
        }
        return mongoDB.getCollection(COLLECTION_URIS);
    }

    public static boolean queueTableExists(MongoDatabase mongoDB) {
        for (String collection : mongoDB.listCollectionNames()) {
            if (collection.toLowerCase().equals(COLLECTION_URIS.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    public static MongoCollection<Document> initDB(MongoDatabase mongoDB) {
        mongoDB.createCollection(COLLECTION_URIS);
        MongoCollection<Document> uriCollection = mongoDB.getCollection(COLLECTION_URIS);
        uriCollection.createIndex(Indexes.ascending(URI_FIELD));
        return uriCollection;
    }
}
