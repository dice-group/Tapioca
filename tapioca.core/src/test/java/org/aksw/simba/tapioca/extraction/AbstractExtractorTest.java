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
