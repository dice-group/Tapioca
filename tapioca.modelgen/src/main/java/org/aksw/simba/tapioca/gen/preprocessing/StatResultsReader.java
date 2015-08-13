package org.aksw.simba.tapioca.gen.preprocessing;

import java.io.FileInputStream;
import java.sql.Timestamp;

import org.aksw.simba.tapioca.extraction.AbstractExtractor;
import org.aksw.simba.tapioca.extraction.RDF2ExtractionStreamer;
import org.aksw.simba.tapioca.gen.data.StatResult;
import org.aksw.simba.tapioca.gen.util.StatResultComparator;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;

public class StatResultsReader {

	private static final Logger LOGGER = LoggerFactory.getLogger(StatResultsReader.class);

	public static final String LOD_STATS_STAT_RESULT_BASE_URI = "http://lodstats.aksw.org/stat_result/";

	// PipedRDFStream and PipedRDFIterator need to be on different threads
	private RDF2ExtractionStreamer streamer = new RDF2ExtractionStreamer();

	public IntObjectOpenHashMap<StatResult> read(String statResultsFile) {
		IntObjectOpenHashMap<StatResult> statResults = readStatResults(statResultsFile);
		statResults = removeOldstatResults(statResults);
		return statResults;
	}

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

	protected IntObjectOpenHashMap<StatResult> removeOldstatResults(IntObjectOpenHashMap<StatResult> statResults) {
		// create mapping dataset -> statResults
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

	protected static class StatResultsExtractor extends AbstractExtractor {

		public static final String FOAF_PRIMARY_TOPIC_URI = FOAF.primaryTopic.getURI();
		public static final String LAST_UPDATED_URI = "http://lodstats.aksw.org/ontology/lastUpdated";

		public IntObjectOpenHashMap<StatResult> statResults = new IntObjectOpenHashMap<StatResult>();

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
								statResult.timestamp = Timestamp.valueOf(triple.getObject().toString()
										.replace('T', ' '));
							} catch (IllegalArgumentException e) {
								LOGGER.error("Couldn't parse time stamp \"" + triple.getObject()
										+ "\". Ignoring this time stamp.");
							}
						}
					}
				}
			}
		}

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
