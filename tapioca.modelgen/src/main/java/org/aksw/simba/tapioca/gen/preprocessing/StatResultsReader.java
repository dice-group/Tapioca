package org.aksw.simba.tapioca.gen.preprocessing;

import java.io.FileReader;
import java.sql.Timestamp;

import org.aksw.simba.tapioca.gen.data.StatResult;
import org.aksw.simba.tapioca.gen.util.StatResultComparator;
import org.apache.commons.io.IOUtils;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.turtle.TurtleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;

public class StatResultsReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatResultsReader.class);

    public static final String LOD_STATS_STAT_RESULT_BASE_URI = "http://lodstats.aksw.org/stat_result/";

    public IntObjectOpenHashMap<StatResult> read(String statResultsFile) {
        IntObjectOpenHashMap<StatResult> statResults = readStatResults(statResultsFile);
        statResults = removeOldstatResults(statResults);
        return statResults;
    }

    private IntObjectOpenHashMap<StatResult> readStatResults(String statResultsFile) {
        RDFParser parser = new TurtleParser();
        StatResultsHandler handler = new StatResultsHandler();
        parser.setRDFHandler(handler);
        parser.setStopAtFirstError(false);
        FileReader reader = null;
        try {
            reader = new FileReader(statResultsFile);
            parser.parse(reader, LOD_STATS_STAT_RESULT_BASE_URI);
        } catch (Exception e) {
            LOGGER.error("Error while parsing file \"" + statResultsFile + "\". Aborting.", e);
            return null;
        } finally {
            IOUtils.closeQuietly(reader);
        }
        return handler.statResults;
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

    private static class StatResultsHandler extends RDFHandlerBase {

        public static final String FOAF_PRIMARY_TOPIC_URI = FOAF.primaryTopic.getURI();
        public static final String LAST_UPDATED_URI = "http://lodstats.aksw.org/ontology/lastUpdated";

        public IntObjectOpenHashMap<StatResult> statResults = new IntObjectOpenHashMap<StatResult>();

        @Override
        public void handleStatement(Statement st) {
            String subjUri = st.getSubject().stringValue();
            int statId;
            StatResult statResult;
            if (subjUri.startsWith(LOD_STATS_STAT_RESULT_BASE_URI)) {
                statId = getDatasetIdFromUri(subjUri);
                if (statId >= 0) {
                    if (statResults.containsKey(statId)) {
                        statResult = statResults.get(statId);
                    } else {
                        statResult = new StatResult(statId, subjUri);
                        statResults.put(statId, statResult);
                    }
                    if (FOAF_PRIMARY_TOPIC_URI.equals(st.getPredicate().stringValue())) {
                        statResult.datasetUri = st.getObject().stringValue();
                    } else if (LAST_UPDATED_URI.equals(st.getPredicate().stringValue())) {
                        try {
                            statResult.timestamp = Timestamp.valueOf(st.getObject().stringValue().replace('T', ' '));
                        } catch (IllegalArgumentException e) {
                            LOGGER.error("Couldn't parse time stamp \"" + st.getObject().stringValue()
                                    + "\". Ignoring this time stamp.");
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
