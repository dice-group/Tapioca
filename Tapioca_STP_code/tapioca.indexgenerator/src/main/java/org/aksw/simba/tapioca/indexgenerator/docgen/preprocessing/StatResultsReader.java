/**
 * This file is part of tapioca.indexgenerator.
 *
 * tapioca.indexgenerator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.indexgenerator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.indexgenerator.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.indexgenerator.docgen.preprocessing;

import java.io.FileInputStream;
import java.sql.Timestamp;

import org.aksw.simba.tapioca.cores.extraction.AbstractExtractor;
import org.aksw.simba.tapioca.cores.extraction.RDF2ExtractionStreamer;
import org.aksw.simba.tapioca.indexgenerator.docgen.data.StatResult;
import org.aksw.simba.tapioca.indexgenerator.docgen.util.StatResultComparator;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
//import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;

public class StatResultsReader {

	// -------------------------------------------------------------------------
	// ------------------ Variables --------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Logger for errors, warnings and other informations.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(StatResultsReader.class);

	/**
	 * base URI
	 */
	public static final String LOD_STATS_STAT_RESULT_BASE_URI = "http://lodstats.aksw.org/stat_result/";

	/**
	 * Piped RDF streamer.
	 */
	private RDF2ExtractionStreamer streamer = new RDF2ExtractionStreamer();

	// -------------------------------------------------------------------------
	// ------------------ Methods ----------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Read additional meta data from statResults file.
	 * 
	 * @param statResultsFile
	 *            statresults.nt, additional meta data file
	 * @return StatResults as IntObjectOpenHashMap
	 */
	public IntObjectOpenHashMap<StatResult> read(String statResultsFile) {
		IntObjectOpenHashMap<StatResult> statResults = readStatResults(statResultsFile);

		// statResults = removeOldstatResults(statResults);

		return statResults;
	}

	/**
	 * Read statResults file
	 * 
	 * @param statResultsFile
	 *            statresults.nt
	 * @return StatResults in extractor
	 */
	private IntObjectOpenHashMap<StatResult> readStatResults(String statResultsFile) {
		StatResultsExtractor extractor = new StatResultsExtractor();
		FileInputStream fin = null;
		try {
			fin = new FileInputStream(statResultsFile);
			streamer.runExtraction(fin, LOD_STATS_STAT_RESULT_BASE_URI, RDFLanguages.filenameToLang(statResultsFile),
					extractor);
		} catch (Exception e) {
			LOGGER.error("Error while parsing file \"" + statResultsFile + "\". Aborting.", e);
			return null;
		} finally {
			IOUtils.closeQuietly(fin);
		}
		return extractor.statResults;
	}

	/**
	 * Filter StatResults.
	 * 
	 * @param statResults
	 * @return filtered statResults
	 */
	protected IntObjectOpenHashMap<StatResult> removeOldstatResults(IntObjectOpenHashMap<StatResult> statResults) {
		// create mapping data set -> statResults
		ObjectObjectOpenHashMap<String, StatResult> datasetStatResultsMapping = new ObjectObjectOpenHashMap<String, StatResult>();
		IntOpenHashSet removedStatResults = new IntOpenHashSet();
		StatResult statResult, statResult2;
		StatResultComparator comparator = new StatResultComparator();
		for (int i = 0; i < statResults.allocated.length; ++i) {
			if (statResults.allocated[i]) {
				statResult = (StatResult) ((Object[]) statResults.values)[i];
				if (datasetStatResultsMapping.containsKey(statResult.datasetUri)) {
					statResult2 = datasetStatResultsMapping.get(statResult.datasetUri);
					if (comparator.compare(statResult, statResult2) > 0) {
						datasetStatResultsMapping.put(statResult.datasetUri, statResult);
						removedStatResults.add(statResult2.id);
						// statResults.remove(statResult2.id);
					} else {
						// statResults.remove(statResult.id);
						removedStatResults.add(statResult.id);
					}
				} else {
					datasetStatResultsMapping.put(statResult.datasetUri, statResult);
				}
			}
		}
		datasetStatResultsMapping = null;
		IntObjectOpenHashMap<StatResult> newStatResults = new IntObjectOpenHashMap<StatResult>();
		for (int i = 0; i < statResults.allocated.length; ++i) {
			if (statResults.allocated[i]) {
				if (!removedStatResults.contains(statResults.keys[i])) {
					newStatResults.put(statResults.keys[i], (StatResult) ((Object[]) statResults.values)[i]);
				}
			}
		}
		return newStatResults;
	}

	// -------------------------------------------------------------------------
	// ------------------ Static Classes ---------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Extract statResults from file
	 */
	protected static class StatResultsExtractor extends AbstractExtractor {

		public static final String FOAF_PRIMARY_TOPIC_URI = FOAF.primaryTopic.getURI();

		public static final String LAST_UPDATED_URI = "http://lodstats.aksw.org/ontology/lastUpdated";

		public IntObjectOpenHashMap<StatResult> statResults = new IntObjectOpenHashMap<StatResult>();

		/**
		 * handle triples read in from file
		 */
		@Override
		public void handleTriple(Triple triple) {
			Node subject = triple.getSubject();
			if (!subject.isBlank()) {
				String subjUri = subject.getURI();
				if (subjUri.startsWith(LOD_STATS_STAT_RESULT_BASE_URI)) {
					int statId = getDatasetIdFromUri(subjUri);
					if (statId >= 0) {
						StatResult statResult;
						if (statResults.containsKey(statId)) {
							statResult = statResults.get(statId);
						} else {
							statResult = new StatResult(statId, subjUri);
							statResults.put(statId, statResult);
						}
						if (triple.getPredicate().equals(FOAF.primaryTopic.asNode())) {
							statResult.datasetUri = triple.getObject().getURI();
						} else if (LAST_UPDATED_URI.equals(triple.getPredicate().getURI())) {
							try {
								// additional "" in timeStamp String, need to be
								// cut off
								String timeStampRaw = triple.getObject().toString().replace("\"", "");
								statResult.timestamp = Timestamp.valueOf(timeStampRaw.replace('T', ' '));
							} catch (IllegalArgumentException e) {
								LOGGER.error("Couldn't parse time stamp \"" + triple.getObject()
										+ "\". Ignoring this time stamp.");
							}
						}
					}
				}
			}
		}

		/**
		 * Return datasetId from URI (mostly the number of the file).
		 * 
		 * @param uri
		 *            the data set URI
		 * @return the datasetId
		 */
		protected static int getDatasetIdFromUri(String uri) {
			try {
				return Integer.parseInt(uri.substring(LOD_STATS_STAT_RESULT_BASE_URI.length()));
			} catch (NumberFormatException e) {
				LOGGER.error("Couldn't extract the stat result id from URI \"" + uri + "\". Returning -1.", e);
				return -1;
			}
		}
	}
}
