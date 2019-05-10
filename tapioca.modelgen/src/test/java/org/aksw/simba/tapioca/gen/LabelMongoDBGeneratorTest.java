package org.aksw.simba.tapioca.gen;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.simba.tapioca.preprocessing.labelretrieving.MongoDBBasedTokenizedLabelRetriever;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;

public class LabelMongoDBGeneratorTest {
    
    public static final String MONGO_HOST = "localhost";
    public static final int MONGO_PORT = 27017;

    @SuppressWarnings("unchecked")
    @Test
    public void test() throws IOException {
        File temp = File.createTempFile("labelTest", "");
        try(Writer writer = new FileWriterWithEncoding(temp, StandardCharsets.UTF_8)) {
            writer.write("http://example.org/A\tA\n" + 
                    "http://example.org/A\ta\n" + 
                    "http://example.org/B\tB\n" + 
                    "http://example.org/A\tA\n" + 
                    "http://example.org/A\taa\n" + 
                    "http://example.org/C\tC\n" + 
                    "http://example.org/B\tb\n");
        }
        Map<String, Set<String>> expectedlabels = new HashMap<>();
        expectedlabels.put("http://example.org/A", new HashSet<>(Arrays.asList("A","a","aa")));
        expectedlabels.put("http://example.org/B", new HashSet<>(Arrays.asList("B","b")));
        expectedlabels.put("http://example.org/C", new HashSet<>(Arrays.asList("C")));
        
        LabelMongoDBGenerator generator = new LabelMongoDBGenerator();
        generator.readTsv(MONGO_HOST, MONGO_PORT, temp.getAbsolutePath());
        
        try (MongoClient client = new MongoClient(MONGO_HOST, MONGO_PORT)) {
            MongoCollection<Document> uriCollection = MongoDBBasedTokenizedLabelRetriever.open(client);
            Iterator<Document> iterator = uriCollection.find().iterator();
            Document document;
            String uri;
            List<String> tokens;
            Set<String> expectedTokens;
            while (iterator.hasNext()) {
                document = iterator.next();
                uri = document.getString(MongoDBBasedTokenizedLabelRetriever.URI_FIELD);
                Assert.assertTrue("unknown URI " + uri, expectedlabels.containsKey(uri));
                tokens = (List<String>) document.get(MongoDBBasedTokenizedLabelRetriever.TOKENS_FIELD);
                expectedTokens = expectedlabels.remove(uri);
                for(String token : tokens) {
                    Assert.assertTrue("unknown token " + uri + " for URI " + uri, expectedTokens.remove(token));
                }
                Assert.assertEquals("Expected tokens couldn't be found " + expectedTokens.toString(), 0, expectedTokens.size());
            }
            Assert.assertEquals("Expected URIs couldn't be found " + expectedlabels.toString(), 0, expectedlabels.size());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
