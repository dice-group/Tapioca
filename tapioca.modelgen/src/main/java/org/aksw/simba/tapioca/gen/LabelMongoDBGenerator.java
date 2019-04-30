package org.aksw.simba.tapioca.gen;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.shaded.com.google.common.collect.Streams;
import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;

/**
 * <b>Note</b> that the file should be sorted by URIs!
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class LabelMongoDBGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(LabelMongoDBGenerator.class);

    private static final String DB_NAME = "tapioca";
    private static final String COLLECTION_URIS = "uris";
    public static final String URI_FIELD = "uri";
    public static final String TOKENS_FIELD = "tokens";

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 3) {
            LOGGER.error("Not enough arguments:\nLabelDBGenerator <mongo-db-host> <mongo-db-port> <input-file>");
            return;
        }
        LabelMongoDBGenerator summarizer = new LabelMongoDBGenerator();
        // for (int i = 1; i < args.length; ++i) {
        // summarizer.addFileOrDir(new File(args[i]));
        // }
        // summarizer.processAsStream(new File(args[1]));
        // summarizer.writeIntoSingleFile(new File(args[1]));
        summarizer.readTsv(args[0], Integer.parseInt(args[1]), args[2]);
    }

    protected void readTsv(String dbHost, int dbPort, String inputFile) {
        try (MongoClient client = new MongoClient(dbHost, dbPort)) {
            MongoCollection<Document> uriCollection = open(client);
            SimpleBuffer buffer = new SimpleBuffer();
            // try (Stream<String> stream = Files.lines(Paths.get(inputFile))) {
            // stream.sequential().forEach(l -> addTsvLine(l, buffer, preparedStmts));
            // } catch (IOException e) {
            // LOGGER.error("Error while reading labels from file.", e);
            // }
            TSVReader tReader = new TSVReader();
            Gson gson = new Gson();
            try (Reader reader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(inputFile), StandardCharsets.UTF_8))) {
                tReader.read(reader, buffer, uriCollection, gson);
            } catch (IOException e) {
                LOGGER.error("Error while reading labels from file.", e);
            }
            // store the last URI
            if (buffer.getUri() != null) {
                store(buffer, uriCollection, gson);
            }
        } catch (Exception e) {
            LOGGER.error("Error while processing file.", e);
        }
    }

    public MongoCollection<Document> open(MongoClient client) {
        MongoDatabase mongoDB = client.getDatabase(DB_NAME);
        if (!queueTableExists(mongoDB)) {
            return initDB(mongoDB);
        }
        return mongoDB.getCollection(COLLECTION_URIS);
    }

    public boolean queueTableExists(MongoDatabase mongoDB) {
        for (String collection : mongoDB.listCollectionNames()) {
            if (collection.toLowerCase().equals(COLLECTION_URIS.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    protected MongoCollection<Document> initDB(MongoDatabase mongoDB) {
        mongoDB.createCollection(COLLECTION_URIS);
        MongoCollection<Document> uriCollection = mongoDB.getCollection(COLLECTION_URIS);
        uriCollection.createIndex(Indexes.ascending(URI_FIELD));
        return uriCollection;
    }

    protected static void store(SimpleBuffer buffer, MongoCollection<Document> uriCollection, Gson gson)
            throws SQLException {
        if (buffer.tokens.isEmpty() || (buffer.uri == null) || buffer.uri.isEmpty()) {
            return;
        }
        Document query = new Document(URI_FIELD, buffer.uri);
        Iterator<Document> iterator = uriCollection.find(query).iterator();
        if (iterator.hasNext()) {
            // Update existing URI
            Document document = iterator.next();
            // deserialize tokens
            @SuppressWarnings("unchecked")
            List<String> tokens = (List<String>) document.get(TOKENS_FIELD);
            tokens = Streams.concat(tokens.stream(), buffer.tokens.stream()).distinct().collect(Collectors.toList());
            Document update = new Document().append("$set", new BasicDBObject().append(TOKENS_FIELD, tokens));
            uriCollection.updateOne(query, update);
        } else {
            // Insert the URI
            query.append(TOKENS_FIELD, buffer.tokens);
            try {
                uriCollection.insertOne(query);
            } catch (MongoWriteException e) {
                if (e.getMessage().contains("key too large to index")) {
                    LOGGER.error("The following URI is too long and will be ignored: {}", buffer.uri);
                } else {
                    throw e;
                }
            }
        }
    }

    public static class SimpleBuffer {
        protected String uri;
        protected List<String> tokens = new ArrayList<>();

        public void add(String token) {
            tokens.add(token);
        }

        public void clear() {
            uri = null;
            tokens.clear();
        }

        /**
         * @return the uri
         */
        public String getUri() {
            return uri;
        }

        /**
         * @param uri
         *            the uri to set
         */
        public void setUri(String uri) {
            this.uri = uri;
        }

        /**
         * @return the tokens
         */
        public List<String> getTokens() {
            return tokens;
        }

        /**
         * @param tokens
         *            the tokens to set
         */
        public void setTokens(List<String> tokens) {
            this.tokens = tokens;
        }
    }

    public static class PreparedInsertStmts implements AutoCloseable {
        public PreparedStatement insertUriStmt;
        public PreparedStatement insertTokenStmt;
        public PreparedStatement insertUriTokenConnectionStmt;

        protected PreparedInsertStmts(PreparedStatement insertUriStmt, PreparedStatement insertTokenStmt,
                PreparedStatement insertUriTokenConnectionStmt) {
            this.insertUriStmt = insertUriStmt;
            this.insertTokenStmt = insertTokenStmt;
            this.insertUriTokenConnectionStmt = insertUriTokenConnectionStmt;
        }

        @Override
        public void close() throws Exception {
            if (insertUriStmt != null) {
                insertUriStmt.close();
            }
            if (insertTokenStmt != null) {
                insertTokenStmt.close();
            }
            if (insertUriTokenConnectionStmt != null) {
                insertUriTokenConnectionStmt.close();
            }
        }

        public static PreparedInsertStmts create(Connection connection) {
            PreparedStatement insertUriStmt;
            PreparedStatement insertTokenStmt;
            PreparedStatement insertUriTokenConnectionStmt;
            try {
                insertUriStmt = connection.prepareStatement("INSERT INTO uris(uri) VALUES(?)");
            } catch (SQLException e) {
                LOGGER.error("Error while creating prepared statement.", e);
                return null;
            }
            try {
                insertTokenStmt = connection.prepareStatement("INSERT INTO tokens(token) VALUES(?)");
            } catch (SQLException e) {
                LOGGER.error("Error while creating prepared statement.", e);
                return null;
            }
            try {
                insertUriTokenConnectionStmt = connection
                        .prepareStatement("INSERT INTO uriTokens(uriId, tokenId) SELECT uriId, tokenId FROM "
                                + "(SELECT id as uriId FROM uris WHERE uri=?),"
                                + "(SELECT id as tokenId FROM tokens WHERE token=?)");
            } catch (SQLException e) {
                LOGGER.error("Error while creating prepared statement.", e);
                return null;
            }
            return new PreparedInsertStmts(insertUriStmt, insertTokenStmt, insertUriTokenConnectionStmt);
        }
    }

    public static class TSVReader {

        public void read(Reader reader, SimpleBuffer buffer, MongoCollection<Document> uriCollection, Gson gson)
                throws IOException {
            StringBuilder builder = new StringBuilder();

            char c;
            while (reader.ready()) {
                c = (char) reader.read();
                switch (c) {
                case '\t': {
                    handleUri(builder.toString(), buffer, uriCollection, gson);
                    builder.delete(0, builder.length());
                    break;
                }
                case '\n': {
                    handleToken(builder.toString(), buffer);
                    builder.delete(0, builder.length());
                    break;
                }
                default: {
                    builder.append(c);
                }
                }
            }
        }

        protected void handleUri(String uri, SimpleBuffer buffer, MongoCollection<Document> uriCollection, Gson gson) {
            if (!uri.equals(buffer.getUri())) {
                // We are done with the current URI, so store its tokens
                try {
                    store(buffer, uriCollection, gson);
                } catch (SQLException e) {
                    LOGGER.error("Error while storing labels in DB.", e);
                }
                buffer.clear();
                buffer.setUri(uri);
            }
        }

        protected void handleToken(String token, SimpleBuffer buffer) {
            buffer.add(token);
        }
    }
}