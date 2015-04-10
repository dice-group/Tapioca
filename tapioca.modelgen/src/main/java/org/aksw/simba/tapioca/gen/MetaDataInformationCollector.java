package org.aksw.simba.tapioca.gen;

import java.io.FileReader;

import org.aksw.simba.tapioca.voidex.DatasetDescription;
import org.apache.commons.io.IOUtils;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.turtle.TurtleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;

public class MetaDataInformationCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataInformationCollector.class);

    private static final String LOD_STATS_DOC_BASE_URI = "http://lodstats.aksw.org/rdfdocs/";

    public void run(String fileName) {
        RDFParser parser = new TurtleParser();
        OnlineLODStatsMetaDataHandler handler = new OnlineLODStatsMetaDataHandler();
        parser.setRDFHandler(handler);
        parser.setStopAtFirstError(false);
        FileReader reader = null;
        try {
            reader = new FileReader(fileName);
            parser.parse(reader, LOD_STATS_DOC_BASE_URI);
        } catch (Exception e) {
            LOGGER.error("Error while parsing file \"" + fileName + "\". Aborting.", e);
            return;
        } finally {
            IOUtils.closeQuietly(reader);
        }
        
        // TODO start handler;
    }

    private static class OnlineLODStatsMetaDataHandler extends RDFHandlerBase {

        public IntObjectOpenHashMap<DatasetDescription> descriptions = new IntObjectOpenHashMap<DatasetDescription>();

        @Override
        public void handleStatement(Statement st) {
            String subjUri = st.getSubject().stringValue();
            int datasetId;
            if (subjUri.startsWith(LOD_STATS_DOC_BASE_URI)) {
                datasetId = getDatasetIdFromUri(subjUri);
                if(datasetId >= 0) {
                    
                }
            }
        }

        protected static int getDatasetIdFromUri(String uri) {
            try {
                return Integer.parseInt(uri.substring(LOD_STATS_DOC_BASE_URI.length()));
            } catch (NumberFormatException e) {
                return -1;
            }
        }
    }
}
