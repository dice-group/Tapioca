package org.aksw.simba.tapioca.gen;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.aksw.simba.tapioca.gen.data.StatResult;
import org.aksw.simba.tapioca.gen.preprocessing.StatResultsReader;
import org.aksw.simba.tapioca.voidex.DatasetDescription;
import org.aksw.simba.topicmodeling.io.CorpusReader;
import org.aksw.simba.topicmodeling.io.gzip.GZipCorpusObjectReader;
import org.aksw.simba.topicmodeling.io.gzip.GZipCorpusObjectWriter;
import org.aksw.simba.topicmodeling.preprocessing.ListCorpusCreator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.CorpusWrappingDocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentFilteringSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.filter.DocumentFilter;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.aksw.simba.topicmodeling.utils.corpus.DocumentListCorpus;
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
import com.hp.hpl.jena.n3.turtle.TurtleReader;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.RDFReader;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.vocabulary.OWL;

public class MetaDataInformationCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataInformationCollector.class);

    private static final String LOD_STATS_DOC_BASE_URI = "http://lodstats.aksw.org/rdfdocs/";

    private static final String LOD_STATS_DATASET_TITLE = "http://lodstats.aksw.org/ontology/extras/catalog-name";

    public static void main(String[] args) {
        MetaDataInformationCollector collector = new MetaDataInformationCollector();
        collector.run("/Daten/Dropbox/lodstats-rdf/23032015/datasets.nt", "/Daten/tapioca/lodStats_BL.object",
                "/Daten/Dropbox/lodstats-rdf/23032015/statresult.nt", "/Daten/tapioca/test.corpus",
                "/Daten/tapioca/lodStats_model/lodstats.nt");
    }

    public void run(String metaFileName, String corpusFileName, String statResultsFile, String corpusOutFileName,
            String additionalMetaDataFile) {
        StatResultsReader reader = new StatResultsReader();
        IntObjectOpenHashMap<StatResult> statResults = reader.read(statResultsFile);
        IntObjectOpenHashMap<DatasetDescription> descriptions = readDescriptions(metaFileName);
        if (additionalMetaDataFile != null) {
            // TODO filter documents
            enricheMetaData(additionalMetaDataFile, descriptions);
        }
        addDescriptionsToCorpus(descriptions, statResults, corpusFileName, corpusOutFileName);
    }

    @SuppressWarnings("unused")
    @Deprecated
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

    protected Corpus readCorpus(String corpusFileName) {
        CorpusReader reader = new GZipCorpusObjectReader(new File(corpusFileName));
        reader.readCorpus();
        return reader.getCorpus();
    }

    protected IntObjectOpenHashMap<DatasetDescription> readDescriptions(String metaFileName) {
        RDFParser parser = new TurtleParser();
        OnlineLODStatsMetaDataHandler handler = new OnlineLODStatsMetaDataHandler();
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

    protected void enricheMetaData(String additionalMetaDataFile, IntObjectOpenHashMap<DatasetDescription> descriptions) {
        // Read additional meta data
        RDFReader reader = new TurtleReader();
        Model model = ModelFactory.createDefaultModel();
        FileInputStream fin = null;
        try {
            fin = new FileInputStream(additionalMetaDataFile);
            reader.read(model, fin, LOD_STATS_DOC_BASE_URI);
        } catch (FileNotFoundException e) {
            LOGGER.error("Couldn't read model with additional meta data from file. Ignoring this file.", e);
            return;
        } finally {
            IOUtils.closeQuietly(fin);
        }
        DatasetDescription description;
        Resource datasetResource;
        com.hp.hpl.jena.rdf.model.Statement s;
        for (int i = 0; i < descriptions.allocated.length; ++i) {
            if (descriptions.allocated[i]) {
                description = (DatasetDescription) ((Object[]) descriptions.values)[i];
                datasetResource = new ResourceImpl(description.uri);
                if (model.containsResource(datasetResource)) {
                    updateDescription(description, datasetResource, model);
                }
                if (model.contains(datasetResource, OWL.sameAs, (RDFNode) null)) {
                    s = model.listStatements(datasetResource, OWL.sameAs, (RDFNode) null).next();
                    datasetResource = s.getObject().asResource();
                    updateDescription(description, datasetResource, model);
                }
                if (model.contains(null, OWL.sameAs, datasetResource)) {
                    s = model.listStatements(null, OWL.sameAs, datasetResource).next();
                    datasetResource = s.getSubject().asResource();
                    updateDescription(description, datasetResource, model);
                }
            }
        }
    }

    protected void updateDescription(DatasetDescription description, Resource datasetResource, Model model) {
        // We prefer the "real" dataset URI
        if (!datasetResource.getURI().startsWith(LOD_STATS_DOC_BASE_URI)) {
            description.uri = datasetResource.getURI();
        }
        // StmtIterator iterator = model.listStatements(datasetResource, null,
        // (RDFNode) null);
        // com.hp.hpl.jena.rdf.model.Statement s;
        // String predicateURI;
        // while (iterator.hasNext()) {
        // s = iterator.next();
        // predicateURI = s.getPredicate().getURI();
        // // if(predicateURI.equals(anObject)) {
        // // TODO
        // // }
        // }
    }

    protected void addDescriptionsToCorpus(IntObjectOpenHashMap<DatasetDescription> descriptions,
            IntObjectOpenHashMap<StatResult> statResults, String corpusFileName, String corpusOutFileName) {
        Corpus corpus1 = readCorpus(corpusFileName);
        DocumentSupplier supplier = new CorpusWrappingDocumentSupplier(corpus1);

        supplier = new DocumentFilteringSupplierDecorator(supplier, new StatResultListBasedDocumentFilter(statResults));
        supplier = new MetaDataAddingSupplierDecorator(supplier, descriptions, statResults);

        ListCorpusCreator<List<Document>> preprocessor = new ListCorpusCreator<List<Document>>(supplier,
                new DocumentListCorpus<List<Document>>(new ArrayList<Document>()));
        Corpus corpus2 = preprocessor.getCorpus();
        corpus2.setProperties(corpus1.getProperties());

        GZipCorpusObjectWriter writer = new GZipCorpusObjectWriter(new File(corpusOutFileName));
        writer.writeCorpus(corpus2);
    }

    protected static class OnlineLODStatsMetaDataHandler extends RDFHandlerBase {

        public static final String DCAT_ACCESS_URL_URI = "http://www.w3.org/ns/dcat#accessURL";
        public static final String DC_SOURCE_URI = DC.SOURCE.stringValue();

        public IntObjectOpenHashMap<DatasetDescription> descriptions = new IntObjectOpenHashMap<DatasetDescription>();

        @Override
        public void handleStatement(Statement st) {
            String subjUri = st.getSubject().stringValue();
            // String docId;
            int datasetId;
            DatasetDescription description;
            if (subjUri.startsWith(LOD_STATS_DOC_BASE_URI)) {
                datasetId = getDatasetIdFromUri(subjUri);
                if (datasetId >= 0) {
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

        protected static int getDatasetIdFromUri(String uri) {
            try {
                return Integer.parseInt(uri.substring(LOD_STATS_DOC_BASE_URI.length()));
            } catch (NumberFormatException e) {
                LOGGER.error("Couldn't extract the dataset id from URI \"" + uri + "\". Returning -1.", e);
                return -1;
            }
        }
    }

    protected static class StatResultListBasedDocumentFilter implements DocumentFilter {

        private IntObjectOpenHashMap<StatResult> statResults;

        public StatResultListBasedDocumentFilter(IntObjectOpenHashMap<StatResult> statResults) {
            this.statResults = statResults;
        }

        @Override
        public boolean isDocumentGood(Document document) {
            DocumentName name = document.getProperty(DocumentName.class);
            int docId = getIdFromDocumentName(name);
            if (docId < 0) {
                LOGGER.warn("Document #" + document.getDocumentId() + " has no id in its name. Removing it.");
                return false;
            }
            return statResults.containsKey(docId);
        }
    }

    protected static class MetaDataAddingSupplierDecorator extends AbstractDocumentSupplierDecorator {

        private IntObjectOpenHashMap<DatasetDescription> descriptions;
        private IntObjectOpenHashMap<StatResult> statResults;

        public MetaDataAddingSupplierDecorator(DocumentSupplier documentSource,
                IntObjectOpenHashMap<DatasetDescription> descriptions, IntObjectOpenHashMap<StatResult> statResults) {
            super(documentSource);
            this.statResults = statResults;
            this.descriptions = descriptions;
        }

        @Override
        protected Document prepareDocument(Document document) {
            DocumentName name = document.getProperty(DocumentName.class);
            int docId = getIdFromDocumentName(name);

            StatResult statResult = statResults.get(docId);
            int datasetId = OnlineLODStatsMetaDataHandler.getDatasetIdFromUri(statResult.getDatasetUri());
            DatasetDescription description;
            if (descriptions.containsKey(datasetId)) {
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
                LOGGER.warn("Document #{} has no description.", datasetId);
                if (document.getProperty(DocumentDescription.class) == null) {
                    document.addProperty(new DocumentDescription("Couldn't get meta data for this dataset."));
                }
            }
            return document;
        }
    }

    protected static int getIdFromDocumentName(DocumentName name) {
        if (name == null) {
            return -1;
        }
        int pos = name.get().indexOf('.');
        if (pos < 0) {
            return -1;
        }
        try {
            return Integer.parseInt(name.get().substring(0, pos));
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
