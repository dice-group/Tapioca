package org.aksw.simba.tapioca.preprocessing;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.data.DatasetSpecialClassesInfo;
import org.aksw.simba.tapioca.data.DatasetVocabularies;
import org.aksw.simba.tapioca.data.DatasetTriplesCount;
import org.aksw.simba.tapioca.data.vocabularies.EVOID;
import org.aksw.simba.tapioca.data.vocabularies.VOID;
import org.aksw.simba.tapioca.voidex.DatasetDescription;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentDescription;
import org.aksw.simba.topicmodeling.utils.doc.DocumentName;
import org.aksw.simba.topicmodeling.utils.doc.DocumentText;
import org.aksw.simba.topicmodeling.utils.doc.DocumentURI;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.turtle.TurtleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;

/**
 * Parses the {@link DocumentText} as RDF VOID information (in turtle). Adds
 * {@link DatasetClassInfo}, {@link DatasetPropertyInfo} and
 * {@link DatasetVocabularies}. May add {@link DocumentURI},
 * {@link DocumentName}, {@link DocumentDescription} if these information are
 * found inside the VOID and the document does not already have the property.
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 * 
 */
public class JenaBasedVoidParsingSupplierDecorator extends AbstractDocumentSupplierDecorator {

    private static final Logger LOGGER = LoggerFactory.getLogger(JenaBasedVoidParsingSupplierDecorator.class);

    public JenaBasedVoidParsingSupplierDecorator(DocumentSupplier documentSource) {
        super(documentSource);
    }

    @Override
    protected Document prepareDocument(Document document) {
        DocumentText text = document.getProperty(DocumentText.class);
        if (text == null) {
            LOGGER.warn("Couldn't get needed DocumentText property from document. Ignoring this document.");
        } else {
            parseVoid(document, text.getText());
        }
        return document;
    }

    private void parseVoid(Document document, String text) {
        RDFParser parser = new TurtleParser();
        OnlineStatementHandler osh = new OnlineStatementHandler();
        parser.setRDFHandler(osh);
        parser.setStopAtFirstError(false);
        String baseURI = extractDatasetNameFromTTL(text);
        if (baseURI == null) {
            baseURI = "";
        }
        try {
            parser.parse(new StringReader(text), baseURI);
        } catch (Exception e) {
            LOGGER.error("Couldn't parse the triples in document " + document.getDocumentId()
                    + ". Returning. Document:(name=" + document.getProperty(DocumentName.class) + ")", e);
            return;
        }

        processDatasetInfos(document, osh.datasetDescriptions);

        ObjectLongOpenHashMap<String> countedURIs = new ObjectLongOpenHashMap<String>(osh.classes.assigned);
        long count;
        for (int i = 0; i < osh.classes.allocated.length; ++i) {
            if (osh.classes.allocated[i]) {
                if (osh.classCounts.containsKey((String) ((Object[]) osh.classes.keys)[i])) {
                    count = osh.classCounts.lget();
                } else {
                    count = 0;
                }
                countedURIs.put((String) ((Object[]) osh.classes.values)[i], count);
            }
        }
        document.addProperty(new DatasetClassInfo(countedURIs));

        countedURIs = new ObjectLongOpenHashMap<String>(osh.specialClasses.assigned);
        for (int i = 0; i < osh.specialClasses.allocated.length; ++i) {
            if (osh.specialClasses.allocated[i]) {
                if (osh.specialClassesCounts.containsKey((String) ((Object[]) osh.specialClasses.keys)[i])) {
                    count = osh.specialClassesCounts.lget();
                } else {
                    count = 0;
                }
                countedURIs.put((String) ((Object[]) osh.specialClasses.values)[i], count);
            }
        }
        document.addProperty(new DatasetSpecialClassesInfo(countedURIs));

        countedURIs = new ObjectLongOpenHashMap<String>();
        for (int i = 0; i < osh.properties.allocated.length; ++i) {
            if (osh.properties.allocated[i]) {
                if (osh.propertyCounts.containsKey((String) ((Object[]) osh.properties.keys)[i])) {
                    count = osh.propertyCounts.lget();
                } else {
                    count = 0;
                }
                countedURIs.put((String) ((Object[]) osh.properties.values)[i], count);
            }
        }
        document.addProperty(new DatasetPropertyInfo(countedURIs));

        document.addProperty(new DatasetVocabularies(osh.vocabularies.toArray(new String[osh.vocabularies.size()])));
    }

    private void processDatasetInfos(Document document,
            ObjectObjectOpenHashMap<String, DatasetDescription> datasetDescriptions) {
        DatasetDescription rootDataset = null;
        if (datasetDescriptions.assigned == 1) {
            for (int i = 0; (rootDataset == null) && (i < datasetDescriptions.allocated.length); ++i) {
                if (datasetDescriptions.allocated[i]) {
                    rootDataset = (DatasetDescription) ((Object[]) datasetDescriptions.values)[i];
                }
            }
            if (document.getProperty(DocumentURI.class) == null) {
                document.addProperty(new DocumentURI(rootDataset.uri));
            }
            if ((rootDataset.title != null) && (document.getProperty(DocumentName.class) == null)) {
                document.addProperty(new DocumentName(rootDataset.title));
            }
            if ((rootDataset.description != null) && (document.getProperty(DocumentDescription.class) == null)) {
                document.addProperty(new DocumentDescription(rootDataset.description));
            }
            document.addProperty(new DatasetTriplesCount(rootDataset.triples));
        } else if (datasetDescriptions.assigned > 1) {
            DatasetDescription temp;
            for (int i = 0; (rootDataset == null) && (i < datasetDescriptions.allocated.length); ++i) {
                if (datasetDescriptions.allocated[i]) {
                    temp = (DatasetDescription) ((Object[]) datasetDescriptions.values)[i];
                    if ((temp.subsets != null) && (temp.subsets.size() > 0)) {
                        rootDataset = temp;
                    }
                }
            }
            if (document.getProperty(DocumentURI.class) == null) {
                document.addProperty(new DocumentURI(rootDataset.uri));
            }
            if ((document.getProperty(DocumentName.class) == null)) {
                if (rootDataset.title == null) {
                    StringBuilder builder = new StringBuilder();
                    builder.append("merged (");
                    boolean first = true;
                    for (DatasetDescription subset : rootDataset.subsets) {
                        if (first) {
                            first = !first;
                        } else {
                            builder.append(',');
                        }
                        builder.append(subset.title != null ? subset.title : subset.uri);
                    }
                    builder.append(')');
                    document.addProperty(new DocumentName(builder.toString()));
                } else {
                    document.addProperty(new DocumentName(rootDataset.title));
                }
            }
            if (document.getProperty(DocumentDescription.class) == null) {
                if (rootDataset.description == null) {
                    StringBuilder builder = new StringBuilder();
                    builder.append("merged description (");
                    for (DatasetDescription subset : rootDataset.subsets) {
                        builder.append('\n');
                        builder.append(subset.title != null ? subset.title : subset.uri);
                        builder.append(":\"");
                        builder.append(subset.description);
                        builder.append('"');
                    }
                    builder.append(')');
                    document.addProperty(new DocumentDescription(builder.toString()));
                } else {
                    document.addProperty(new DocumentDescription(rootDataset.description));
                }
            }
            long triples = 0;
            if (rootDataset.triples == -1) {
                for (DatasetDescription subset : rootDataset.subsets) {
                    triples += subset.triples;
                }
            } else {
                triples = rootDataset.triples;
            }
            document.addProperty(new DatasetTriplesCount(triples));
        } else {
            LOGGER.warn("Couldn't find a URI for the dataset in document " + document.getDocumentId()
                    + ". Document:(name=" + document.getProperty(DocumentName.class) + ")");
        }
    }

    private String extractDatasetNameFromTTL(String text) {
        int start = text.indexOf("\n\n<");
        if (start < 0) {
            return null;
        }
        start += 3;
        int end = text.indexOf('>', start);
        if (end < 0) {
            return null;
        } else {
            return text.substring(start, end);
        }
    }

    private static class OnlineStatementHandler extends RDFHandlerBase {

        private static final String RDF_TYPE_URI = RDF.type.getURI();
        private static final String DATASET_URI = VOID.dataset.getURI();
        private static final String CLASS_PROPERTY_URI = VOID.clazz.getURI();
        private static final String ENTITIES_COUNT_PROPERTY_URI = VOID.entities.getURI();
        private static final String SPECIAL_CLASS_PROPERTY_URI = EVOID.specialClass.getURI();
        private static final String SPECIAL_ENTITIES_COUNT_PROPERTY_URI = EVOID.entities.getURI();
        private static final String PROPERTY_PROPERTY_URI = VOID.property.getURI();
        private static final String TRIPLES_COUNT_PROPERTY_URI = VOID.triples.getURI();
        private static final String VOCABULARY_PROPERTY_URI = VOID.vocabulary.getURI();
        private static final String TITLE_PROPERTY_URI = DCTerms.title.getURI();
        private static final String SUBSET_PROPERTY_URI = VOID.subset.getURI();
        private static final String DESCRIPTION_PROPERTY_URI = DCTerms.description.getURI();

        public ObjectObjectOpenHashMap<String, String> classes = new ObjectObjectOpenHashMap<String, String>();
        public ObjectLongOpenHashMap<String> classCounts = new ObjectLongOpenHashMap<String>();
        public ObjectObjectOpenHashMap<String, String> specialClasses = new ObjectObjectOpenHashMap<String, String>();
        public ObjectLongOpenHashMap<String> specialClassesCounts = new ObjectLongOpenHashMap<String>();
        public ObjectObjectOpenHashMap<String, String> properties = new ObjectObjectOpenHashMap<String, String>();
        public ObjectLongOpenHashMap<String> propertyCounts = new ObjectLongOpenHashMap<String>();
        public List<String> vocabularies = new ArrayList<String>();
        public ObjectObjectOpenHashMap<String, DatasetDescription> datasetDescriptions = new ObjectObjectOpenHashMap<String, DatasetDescription>();

        @Override
        public void handleStatement(Statement st) {
            try {
                String predicate = st.getPredicate().stringValue();
                if (st.getObject().stringValue().toLowerCase().equals(DATASET_URI)
                        && (predicate.equals(RDF_TYPE_URI))) {
                    String datasetUri = st.getSubject().stringValue();
                    if (!datasetDescriptions.containsKey(datasetUri)) {
                        datasetDescriptions.put(datasetUri, new DatasetDescription(datasetUri));
                    }
                } else if (predicate.equals(CLASS_PROPERTY_URI)) {
                    classes.put(st.getSubject().stringValue(), st.getObject().stringValue());
                } else if (predicate.equals(ENTITIES_COUNT_PROPERTY_URI)) {
                    classCounts.put(st.getSubject().stringValue(), Long.parseLong(st.getObject().stringValue()));
                } else if (predicate.equals(SPECIAL_CLASS_PROPERTY_URI)) {
                    specialClasses.put(st.getSubject().stringValue(), st.getObject().stringValue());
                } else if (predicate.equals(SPECIAL_ENTITIES_COUNT_PROPERTY_URI)) {
                    specialClassesCounts.put(st.getSubject().stringValue(),
                            Long.parseLong(st.getObject().stringValue()));
                } else if (predicate.equals(PROPERTY_PROPERTY_URI)) {
                    properties.put(st.getSubject().stringValue(), st.getObject().stringValue());
                } else if (predicate.equals(TRIPLES_COUNT_PROPERTY_URI)) {
                    String subject = st.getSubject().stringValue();
                    if (datasetDescriptions.containsKey(subject)) {
                        datasetDescriptions.get(subject).triples = Long.parseLong(st.getObject().stringValue());
                    } else {
                        propertyCounts.put(subject, Long.parseLong(st.getObject().stringValue()));
                    }
                } else if (predicate.equals(VOCABULARY_PROPERTY_URI)) {
                    String object = st.getObject().stringValue().trim();
                    vocabularies.add(object);
                } else if (predicate.equals(TITLE_PROPERTY_URI)) {
                    String datasetUri = st.getSubject().stringValue();
                    DatasetDescription description;
                    if (!datasetDescriptions.containsKey(datasetUri)) {
                        description = new DatasetDescription(datasetUri);
                        datasetDescriptions.put(datasetUri, description);
                    } else {
                        description = datasetDescriptions.get(datasetUri);
                    }
                    description.title = st.getObject().stringValue();
                } else if (predicate.equals(SUBSET_PROPERTY_URI)) {
                    String dataset1Uri = st.getSubject().stringValue();
                    String dataset2Uri = st.getObject().stringValue();
                    DatasetDescription description1, description2;
                    if (!datasetDescriptions.containsKey(dataset1Uri)) {
                        description1 = new DatasetDescription(dataset1Uri);
                        datasetDescriptions.put(dataset1Uri, description1);
                    } else {
                        description1 = datasetDescriptions.get(dataset1Uri);
                    }
                    if (!datasetDescriptions.containsKey(dataset2Uri)) {
                        description2 = new DatasetDescription(dataset2Uri);
                        datasetDescriptions.put(dataset1Uri, description2);
                    } else {
                        description2 = datasetDescriptions.get(dataset2Uri);
                    }
                    description1.addSubset(description2);
                } else if (predicate.equals(DESCRIPTION_PROPERTY_URI)) {
                    String datasetUri = st.getSubject().stringValue();
                    DatasetDescription description;
                    if (!datasetDescriptions.containsKey(datasetUri)) {
                        description = new DatasetDescription(datasetUri);
                        datasetDescriptions.put(datasetUri, description);
                    } else {
                        description = datasetDescriptions.get(datasetUri);
                    }
                    description.description = st.getObject().stringValue();
                } else if (predicate.equals(CLASS_PROPERTY_URI)) {
                    classes.put(st.getSubject().stringValue(), st.getObject().stringValue());
                }
            } catch (Exception e) {
                LOGGER.error("Couldn't parse the triple \"" + st.toString() + "\".", e);
            }
        }
    }
}
