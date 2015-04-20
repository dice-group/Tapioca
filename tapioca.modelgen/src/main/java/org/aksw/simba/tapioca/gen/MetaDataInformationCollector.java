package org.aksw.simba.tapioca.gen;

import java.io.File;
import java.io.FileReader;

import org.aksw.simba.tapioca.voidex.DatasetDescription;
import org.aksw.simba.topicmodeling.io.CorpusObjectReader;
import org.aksw.simba.topicmodeling.io.CorpusReader;
import org.aksw.simba.topicmodeling.io.gzip.GZipCorpusObjectWriter;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentDescription;
import org.aksw.simba.topicmodeling.utils.doc.DocumentName;
import org.aksw.simba.topicmodeling.utils.doc.DocumentURI;
import org.apache.commons.io.IOUtils;
import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.DC;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.turtle.TurtleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectIntOpenHashMap;

public class MetaDataInformationCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataInformationCollector.class);

    private static final String LOD_STATS_DOC_BASE_URI = "http://lodstats.aksw.org/rdfdocs/";

    public void run(String metaFileName, String corpusFileName, String corpusOutFileName) {
        ObjectIntOpenHashMap<String> docIdCorpusIdMapping = createDocIdCorpusIdMapping(corpusFileName);
        IntObjectOpenHashMap<DatasetDescription> descriptions = readDescriptions(metaFileName, docIdCorpusIdMapping);
        addDescriptionsToCorpus(descriptions, corpusFileName, corpusOutFileName);
    }

    private ObjectIntOpenHashMap<String> createDocIdCorpusIdMapping(String corpusFileName) {
        ObjectIntOpenHashMap<String> mapping = new ObjectIntOpenHashMap<String>();
        Corpus corpus = readCorpus(corpusFileName);
        DocumentName name;
        String docId;
        int pos, id = 0;
        for (Document document : corpus) {
            name = document.getProperty(DocumentName.class);
            if (name != null) {
                docId = name.get();
                pos = docId.indexOf('.');
                if (pos > 0) {
                    docId = new String(docId.substring(0, pos));
                }
                mapping.put(docId, id);
            } else {
                LOGGER.warn("Document #" + id + " has no DocumentName property.");
            }
            ++id;
        }
        return mapping;
    }

    private Corpus readCorpus(String corpusFileName) {
        CorpusReader reader = new CorpusObjectReader(new File(corpusFileName));
        reader.readCorpus();
        return reader.getCorpus();
    }

    private IntObjectOpenHashMap<DatasetDescription> readDescriptions(String metaFileName,
            ObjectIntOpenHashMap<String> docIdCorpusIdMapping) {
        RDFParser parser = new TurtleParser();
        OnlineLODStatsMetaDataHandler handler = new OnlineLODStatsMetaDataHandler(docIdCorpusIdMapping);
        parser.setRDFHandler(handler);
        parser.setStopAtFirstError(false);
        FileReader reader = null;
        try {
            reader = new FileReader(metaFileName);
            parser.parse(reader, LOD_STATS_DOC_BASE_URI);
        } catch (Exception e) {
            LOGGER.error("Error while parsing file \"" + metaFileName + "\". Aborting.", e);
            return null;
        } finally {
            IOUtils.closeQuietly(reader);
        }
        return handler.descriptions;
    }

    private void addDescriptionsToCorpus(IntObjectOpenHashMap<DatasetDescription> descriptions, String corpusFileName,
            String corpusOutFileName) {
        Corpus corpus = readCorpus(corpusFileName);
        int id = 0;
        DatasetDescription description;
        for (Document document : corpus) {
            if (descriptions.containsKey(id)) {
                description = descriptions.lget();
                if (description.title != null) {
                    document.addProperty(new DocumentName(description.title));
                }
                if (description.uri != null) {
                    document.addProperty(new DocumentURI(description.uri));
                }
                if (description.description != null) {
                    document.addProperty(new DocumentDescription(description.description));
                }
            } else {
                LOGGER.warn("Document #{} has no description.", id);
                if (document.getProperty(DocumentDescription.class) == null) {
                    document.addProperty(new DocumentDescription("Couldn't get meta data for this dataset."));
                }
            }
            ++id;
        }

        GZipCorpusObjectWriter writer = new GZipCorpusObjectWriter(new File(corpusOutFileName));
        writer.writeCorpus(corpus);
    }

    private static class OnlineLODStatsMetaDataHandler extends RDFHandlerBase {

        public static final String DCAT_ACCESS_URL_URI = "http://www.w3.org/ns/dcat#accessURL";
        public static final String DC_SOURCE_URI = DC.SOURCE.stringValue();

        public IntObjectOpenHashMap<DatasetDescription> descriptions = new IntObjectOpenHashMap<DatasetDescription>();
        public ObjectIntOpenHashMap<String> docIdCorpusIdMapping;

        public OnlineLODStatsMetaDataHandler(ObjectIntOpenHashMap<String> docIdCorpusIdMapping) {
            this.docIdCorpusIdMapping = docIdCorpusIdMapping;
        }

        @Override
        public void handleStatement(Statement st) {
            String subjUri = st.getSubject().stringValue();
            String docId;
            int datasetId;
            DatasetDescription description;
            if (subjUri.startsWith(LOD_STATS_DOC_BASE_URI)) {
                docId = getDatasetIdFromUri(subjUri);
                if (docIdCorpusIdMapping.containsKey(docId)) {
                    datasetId = docIdCorpusIdMapping.get(docId);
                    if (descriptions.containsKey(datasetId)) {
                        description = descriptions.get(datasetId);
                    } else {
                        description = new DatasetDescription(subjUri);
                        descriptions.put(datasetId, description);
                    }
                    if (DCAT_ACCESS_URL_URI.equals(st.getPredicate().stringValue())) {
                        description.title = st.getObject().stringValue();
                    } else if (DC_SOURCE_URI.equals(st.getPredicate().stringValue())) {
                        description.description = "Accessed through " + st.getObject().stringValue();
                    }
                }
            }
        }

        protected static String getDatasetIdFromUri(String uri) {
            return uri.substring(LOD_STATS_DOC_BASE_URI.length());
        }
    }
}
