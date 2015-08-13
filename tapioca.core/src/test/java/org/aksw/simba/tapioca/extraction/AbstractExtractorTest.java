package org.aksw.simba.tapioca.extraction;

import java.util.Arrays;

import junit.framework.Assert;

import org.apache.jena.riot.Lang;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

public class AbstractExtractorTest {

	public void runTestOnN3(String rdfData, Extractor extractor) {
		RDF2ExtractionStreamer streamer = new RDF2ExtractionStreamer();
		streamer.runExtraction(rdfData, "", Lang.N3, extractor);
	}

	public void runTestOnTTL(String rdfData, Extractor extractor) {
		RDF2ExtractionStreamer streamer = new RDF2ExtractionStreamer();
		streamer.runExtraction(rdfData, "", Lang.TTL, extractor);
	}

	public void checkExtractedData(ObjectIntOpenHashMap<String> countedUris, String[] expectedUris, int[] expectedCounts) {
		Assert.assertEquals("Expected the same length. Map: " + countedUris.toString() + "   expected Array : "
				+ Arrays.toString(expectedUris), expectedUris.length, countedUris.size());
		for (int i = 0; i < expectedUris.length; ++i) {
			Assert.assertTrue("Couldn't find " + expectedUris[i], countedUris.containsKey(expectedUris[i]));
			Assert.assertEquals("Wrong count for " + expectedUris[i], expectedCounts[i], countedUris.lget());
		}
	}
}
