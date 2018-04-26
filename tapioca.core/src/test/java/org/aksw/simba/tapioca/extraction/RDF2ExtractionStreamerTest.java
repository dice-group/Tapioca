/**
 * tapioca.core - ${project.description}
 * Copyright © 2015 Data Science Group (DICE) (michael.roeder@uni-paderborn.de)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * This file is part of tapioca.core.
 *
 * tapioca.core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.core.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.extraction;

import java.io.InputStream;
import java.util.Arrays;

import org.aksw.simba.tapioca.extraction.voidex.VoidExtractor;
import org.aksw.simba.topicmodeling.concurrent.utils.MaxDurationCheckingThread;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.junit.Test;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

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
