package org.aksw.simba.tapioca.preprocessing;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.data.DatasetSpecialClassesInfo;
import org.aksw.simba.tapioca.data.DatasetTriplesCount;
import org.aksw.simba.tapioca.data.DatasetVocabularies;
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
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;

/**
 * Parses the {@link DocumentText} as RDF VOID information (in turtle). Adds
 * {@link DatasetClassInfo}, {@link DatasetPropertyInfo} and
 * {@link DatasetVocabularies}. Can add/replace {@link DocumentURI},
 * {@link DocumentName}, {@link DocumentDescription}.
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 * 
 */
public class JenaBasedVoidParsingSupplierDecorator extends AbstractDocumentSupplierDecorator {

	private static final Logger LOGGER = LoggerFactory.getLogger(JenaBasedVoidParsingSupplierDecorator.class);

	// PipedRDFStream and PipedRDFIterator need to be on different threads
	private ExecutorService executor = Executors.newSingleThreadExecutor();

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
		PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
		final PipedRDFStream<Triple> rdfStream = new PipedTriplesStream(iter);
		// RDFParser parser = new TurtleParser();
		// OnlineStatementHandler osh = new OnlineStatementHandler();
		// parser.setRDFHandler(osh);
		// parser.setStopAtFirstError(false);
		String baseURI = extractDatasetNameFromTTL(text);
		if (baseURI == null) {
			baseURI = "";
		}
		try {
			ParserTask parserTask = new ParserTask(rdfStream, new StringReader(text), baseURI);
			executor.execute(parserTask);
			// parser.parse(new StringReader(text), baseURI);
			addVoidInfoToDoc(document, iter);
		} catch (Exception e) {
			LOGGER.error("Couldn't parse the triples in document " + document.getDocumentId()
					+ ". Returning. Document:(name=" + document.getProperty(DocumentName.class) + ")", e);
			return;
		}
	}

	private void addVoidInfoToDoc(Document document, PipedRDFIterator<Triple> iter) {
		ObjectObjectOpenHashMap<String, String> classes = new ObjectObjectOpenHashMap<String, String>();
		ObjectLongOpenHashMap<String> classCounts = new ObjectLongOpenHashMap<String>();
		ObjectObjectOpenHashMap<String, String> specialClasses = new ObjectObjectOpenHashMap<String, String>();
		ObjectLongOpenHashMap<String> specialClassesCounts = new ObjectLongOpenHashMap<String>();
		ObjectObjectOpenHashMap<String, String> properties = new ObjectObjectOpenHashMap<String, String>();
		ObjectLongOpenHashMap<String> propertyCounts = new ObjectLongOpenHashMap<String>();
		List<String> vocabularies = new ArrayList<String>();
		ObjectObjectOpenHashMap<String, DatasetDescription> datasetDescriptions = new ObjectObjectOpenHashMap<String, DatasetDescription>();

		Triple triple;
		Node subject, predicate, object;
		while (iter.hasNext()) {
			triple = iter.next();
			try {
				subject = triple.getSubject();
				predicate = triple.getPredicate();
				object = triple.getObject();
				if (object.equals(VOID.dataset) && (predicate.equals(RDF.type))) {
					String datasetUri = subject.getURI();
					if (!datasetDescriptions.containsKey(datasetUri)) {
						datasetDescriptions.put(datasetUri, new DatasetDescription(datasetUri));
					}
				} else if (predicate.equals(VOID.clazz)) {
					classes.put(subject.getURI(), object.getURI());
				} else if (predicate.equals(VOID.entities)) {
					classCounts.put(subject.getURI(), Long.parseLong(object.toString()));
				} else if (predicate.equals(EVOID.specialClass)) {
					specialClasses.put(subject.getURI(), object.getURI());
				} else if (predicate.equals(EVOID.entities)) {
					specialClassesCounts.put(subject.getURI(), Long.parseLong(object.toString()));
				} else if (predicate.equals(VOID.property)) {
					properties.put(subject.getURI(), object.getURI());
				} else if (predicate.equals(VOID.triples)) {
					String sub = subject.getURI();
					if (datasetDescriptions.containsKey(sub)) {
						datasetDescriptions.get(sub).triples = Long.parseLong(object.toString());
					} else {
						propertyCounts.put(sub, Long.parseLong(object.toString()));
					}
				} else if (predicate.equals(VOID.vocabulary)) {
					vocabularies.add(object.toString());
				} else if (predicate.equals(DCTerms.title)) {
					String datasetUri = subject.getURI();
					DatasetDescription description;
					if (!datasetDescriptions.containsKey(datasetUri)) {
						description = new DatasetDescription(datasetUri);
						datasetDescriptions.put(datasetUri, description);
					} else {
						description = datasetDescriptions.get(datasetUri);
					}
					description.title = object.toString();
				} else if (predicate.equals(VOID.subset)) {
					String dataset1Uri = subject.getURI();
					String dataset2Uri = object.getURI();
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
				} else if (predicate.equals(DCTerms.description)) {
					String datasetUri = subject.getURI();
					DatasetDescription description;
					if (!datasetDescriptions.containsKey(datasetUri)) {
						description = new DatasetDescription(datasetUri);
						datasetDescriptions.put(datasetUri, description);
					} else {
						description = datasetDescriptions.get(datasetUri);
					}
					description.description = object.toString();
				}
			} catch (Exception e) {
				LOGGER.error("Couldn't parse the triple \"" + triple + "\".", e);
			}
		}

		ObjectLongOpenHashMap<String> countedURIs = new ObjectLongOpenHashMap<String>(classes.assigned);
		long count;
		for (int i = 0; i < classes.allocated.length; ++i) {
			if (classes.allocated[i]) {
				if (classCounts.containsKey((String) ((Object[]) classes.keys)[i])) {
					count = classCounts.lget();
				} else {
					count = 0;
				}
				countedURIs.put((String) ((Object[]) classes.values)[i], count);
			}
		}
		document.addProperty(new DatasetClassInfo(countedURIs));

		countedURIs = new ObjectLongOpenHashMap<String>(specialClasses.assigned);
		for (int i = 0; i < specialClasses.allocated.length; ++i) {
			if (specialClasses.allocated[i]) {
				if (specialClassesCounts.containsKey((String) ((Object[]) specialClasses.keys)[i])) {
					count = specialClassesCounts.lget();
				} else {
					count = 0;
				}
				countedURIs.put((String) ((Object[]) specialClasses.values)[i], count);
			}
		}
		document.addProperty(new DatasetSpecialClassesInfo(countedURIs));

		countedURIs = new ObjectLongOpenHashMap<String>();
		for (int i = 0; i < properties.allocated.length; ++i) {
			if (properties.allocated[i]) {
				if (propertyCounts.containsKey((String) ((Object[]) properties.keys)[i])) {
					count = propertyCounts.lget();
				} else {
					count = 0;
				}
				countedURIs.put((String) ((Object[]) properties.values)[i], count);
			}
		}
		document.addProperty(new DatasetPropertyInfo(countedURIs));

		document.addProperty(new DatasetVocabularies(vocabularies.toArray(new String[vocabularies.size()])));

		processDatasetInfos(document, datasetDescriptions);
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
			document.addProperty(new DocumentURI(rootDataset.uri));
			if (rootDataset.title != null) {
				document.addProperty(new DocumentName(rootDataset.title));
			}
			if (rootDataset.description != null) {
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
			document.addProperty(new DocumentURI(rootDataset.uri));
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

	private static class ParserTask implements Runnable {

		private PipedRDFStream<Triple> rdfStream;
		private StringReader reader;
		private String baseUri;

		public ParserTask(PipedRDFStream<Triple> rdfStream, StringReader reader, String baseUri) {
			this.rdfStream = rdfStream;
			this.reader = reader;
			this.baseUri = baseUri;
		}

		@Override
		public void run() {
			// Call the parsing process.
			RDFDataMgr.parse(rdfStream, reader, baseUri, Lang.TTL);
		}
	};

	// private static class OnlineStatementHandler extends RDFHandlerBase {
	//
	// private static final String RDF_TYPE_URI = RDF.type.getURI();
	// private static final String DATASET_URI = VOID.dataset.getURI();
	// private static final String CLASS_PROPERTY_URI = VOID.clazz.getURI();
	// private static final String ENTITIES_COUNT_PROPERTY_URI =
	// VOID.entities.getURI();
	// private static final String SPECIAL_CLASS_PROPERTY_URI =
	// EVOID.specialClass.getURI();
	// private static final String SPECIAL_ENTITIES_COUNT_PROPERTY_URI =
	// EVOID.entities.getURI();
	// private static final String PROPERTY_PROPERTY_URI =
	// VOID.property.getURI();
	// private static final String TRIPLES_COUNT_PROPERTY_URI =
	// VOID.triples.getURI();
	// private static final String VOCABULARY_PROPERTY_URI =
	// VOID.vocabulary.getURI();
	// private static final String TITLE_PROPERTY_URI = DCTerms.title.getURI();
	// private static final String SUBSET_PROPERTY_URI = VOID.subset.getURI();
	// private static final String DESCRIPTION_PROPERTY_URI =
	// DCTerms.description.getURI();
	//
	// public ObjectObjectOpenHashMap<String, String> classes = new
	// ObjectObjectOpenHashMap<String, String>();
	// public ObjectLongOpenHashMap<String> classCounts = new
	// ObjectLongOpenHashMap<String>();
	// public ObjectObjectOpenHashMap<String, String> specialClasses = new
	// ObjectObjectOpenHashMap<String, String>();
	// public ObjectLongOpenHashMap<String> specialClassesCounts = new
	// ObjectLongOpenHashMap<String>();
	// public ObjectObjectOpenHashMap<String, String> properties = new
	// ObjectObjectOpenHashMap<String, String>();
	// public ObjectLongOpenHashMap<String> propertyCounts = new
	// ObjectLongOpenHashMap<String>();
	// public List<String> vocabularies = new ArrayList<String>();
	// public ObjectObjectOpenHashMap<String, DatasetDescription>
	// datasetDescriptions = new ObjectObjectOpenHashMap<String,
	// DatasetDescription>();
	//
	// @Override
	// public void handleStatement(Statement st) {
	// }
	// }
}
