package org.aksw.simba.tapioca.extraction;

import java.io.InputStream;
import java.util.Arrays;

import org.aksw.simba.tapioca.extraction.voidex.VoidExtractor;
import org.aksw.simba.topicmodeling.concurrent.utils.MaxDurationCheckingThread;
import org.apache.jena.riot.Lang;
import org.junit.Test;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import junit.framework.Assert;

public class RDF2ExtractionStreamerTest {

    @Test
    public void testStreaming2VoidExtraction() {
        String data = "<http://example.org/entity1> <http://example.org/hasLabel> \"entity 1\" .\n"
                + "<http://example.org/entity1> <" + RDF.type.getURI() + "> <http://example.org/Class1> .\n"
                + "<http://example.org/Class2> <" + RDF.type.getURI() + "> <" + RDFS.Class.getURI() + "> .\n";
        String expectedClasses[] = new String[] { "http://example.org/Class1", "http://example.org/Class2",
                RDFS.Class.getURI() };
        int expectedCounts[] = new int[] { 1, 0, 1 };
        String expectedProperties[] = new String[] { RDF.type.getURI(), "http://example.org/hasLabel" };
        int expectedPropertyCounts[] = new int[] { 2, 1 };

        MaxDurationCheckingThread checkingThread = new MaxDurationCheckingThread(Thread.currentThread(), 1000);
        VoidExtractor extractor = new VoidExtractor();
        RDF2ExtractionStreamer streamer = new RDF2ExtractionStreamer();

        (new Thread(checkingThread)).start();
        streamer.runExtraction(data, "", Lang.N3, extractor);

        ObjectIntOpenHashMap<String> countedUris = extractor.getCountedClasses();
        Assert.assertEquals("Expected the same length. Map: " + countedUris.toString() + "   expected Array : "
                + Arrays.toString(expectedClasses), expectedClasses.length, countedUris.size());
        for (int i = 0; i < expectedClasses.length; ++i) {
            Assert.assertTrue("Couldn't find " + expectedClasses[i], countedUris.containsKey(expectedClasses[i]));
            Assert.assertEquals("Wrong count for " + expectedClasses[i], expectedCounts[i], countedUris.lget());
        }
        countedUris = extractor.getCountedProperties();
        Assert.assertEquals("Expected the same length. Map: " + countedUris.toString() + "   expected Array : "
                + Arrays.toString(expectedProperties), expectedProperties.length, countedUris.size());
        for (int i = 0; i < expectedProperties.length; ++i) {
            Assert.assertTrue("Couldn't find " + expectedProperties[i], countedUris.containsKey(expectedProperties[i]));
            Assert.assertEquals("Wrong count for " + expectedProperties[i], expectedPropertyCounts[i],
                    countedUris.lget());
        }
        checkingThread.reportFinished();
    }

    @Test
    public void testBrokenStream2VoidExtraction() {
        System.out.println("This test might generate exceptions...");
        InputStream data = null;
        String expectedClasses[] = new String[0];
        int expectedCounts[] = new int[0];
        String expectedProperties[] = new String[0];
        int expectedPropertyCounts[] = new int[0];

        MaxDurationCheckingThread checkingThread = new MaxDurationCheckingThread(Thread.currentThread(), 1000);
        VoidExtractor extractor = new VoidExtractor();
        RDF2ExtractionStreamer streamer = new RDF2ExtractionStreamer();
        
        (new Thread(checkingThread)).start();
        streamer.runExtraction(data, "", Lang.N3, extractor);

        ObjectIntOpenHashMap<String> countedUris = extractor.getCountedClasses();
        Assert.assertEquals("Expected the same length. Map: " + countedUris.toString() + "   expected Array : "
                + Arrays.toString(expectedClasses), expectedClasses.length, countedUris.size());
        for (int i = 0; i < expectedClasses.length; ++i) {
            Assert.assertTrue("Couldn't find " + expectedClasses[i], countedUris.containsKey(expectedClasses[i]));
            Assert.assertEquals("Wrong count for " + expectedClasses[i], expectedCounts[i], countedUris.lget());
        }
        countedUris = extractor.getCountedProperties();
        Assert.assertEquals("Expected the same length. Map: " + countedUris.toString() + "   expected Array : "
                + Arrays.toString(expectedProperties), expectedProperties.length, countedUris.size());
        for (int i = 0; i < expectedProperties.length; ++i) {
            Assert.assertTrue("Couldn't find " + expectedProperties[i], countedUris.containsKey(expectedProperties[i]));
            Assert.assertEquals("Wrong count for " + expectedProperties[i], expectedPropertyCounts[i],
                    countedUris.lget());
        }
        checkingThread.reportFinished();
    }
}
