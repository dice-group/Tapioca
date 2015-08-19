package org.aksw.simba.tapioca.preprocessing;

import java.util.ArrayList;
import java.util.List;

import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.data.DatasetSpecialClassesInfo;
import org.aksw.simba.tapioca.data.DatasetTriplesCount;
import org.aksw.simba.tapioca.data.DatasetVocabularies;
import org.aksw.simba.tapioca.data.vocabularies.EVOID;
import org.aksw.simba.tapioca.data.vocabularies.VOID;
import org.aksw.simba.tapioca.extraction.AbstractExtractor;
import org.aksw.simba.tapioca.extraction.RDF2ExtractionStreamer;
import org.aksw.simba.tapioca.extraction.voidex.DatasetDescription;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentDescription;
import org.aksw.simba.topicmodeling.utils.doc.DocumentName;
import org.aksw.simba.topicmodeling.utils.doc.DocumentText;
import org.aksw.simba.topicmodeling.utils.doc.DocumentURI;
import org.apache.jena.riot.Lang;
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
 * {@link DatasetVocabularies}. May add {@link DocumentURI},
 * {@link DocumentName}, {@link DocumentDescription} if these information are
 * found inside the VOID and the document does not already have the property.
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 * 
 */
public class JenaBasedVoidParsingSupplierDecorator extends AbstractDocumentSupplierDecorator {

	private static final Logger LOGGER = LoggerFactory.getLogger(JenaBasedVoidParsingSupplierDecorator.class);

	// PipedRDFStream and PipedRDFIterator need to be on different threads
	private RDF2ExtractionStreamer streamer = new RDF2ExtractionStreamer();

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
		String baseURI = extractDatasetNameFromTTL(text);
		if (baseURI == null) {
			baseURI = "";
		}
		VoidParser parser = new VoidParser();
		streamer.runExtraction(text, baseURI, Lang.TTL, parser);
		parser.addVoidToDocument(document);
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

	protected class VoidParser extends AbstractExtractor {
		private ObjectObjectOpenHashMap<String, String> classes = new ObjectObjectOpenHashMap<String, String>();
		private ObjectLongOpenHashMap<String> classCounts = new ObjectLongOpenHashMap<String>();
		private ObjectObjectOpenHashMap<String, String> specialClasses = new ObjectObjectOpenHashMap<String, String>();
		private ObjectLongOpenHashMap<String> specialClassesCounts = new ObjectLongOpenHashMap<String>();
		private ObjectObjectOpenHashMap<String, String> properties = new ObjectObjectOpenHashMap<String, String>();
		private ObjectLongOpenHashMap<String> propertyCounts = new ObjectLongOpenHashMap<String>();
		private List<String> vocabularies = new ArrayList<String>();
		private ObjectObjectOpenHashMap<String, DatasetDescription> datasetDescriptions = new ObjectObjectOpenHashMap<String, DatasetDescription>();

		@Override
		public void handleTriple(Triple triple) {
			try {
				Node subject = triple.getSubject();
				Node predicate = triple.getPredicate();
				Node object = triple.getObject();
				if (object.equals(VOID.Dataset.asNode()) && (predicate.equals(RDF.type.asNode()))) {
					String datasetUri = subject.getURI();
					if (!datasetDescriptions.containsKey(datasetUri)) {
						datasetDescriptions.put(datasetUri, new DatasetDescription(datasetUri));
					}
				} else if (predicate.equals(VOID.clazz.asNode())) {
					classes.put(subject.getBlankNodeLabel(), object.isBlank() ? object.getBlankNodeLabel() : object.getURI());
				} else if (predicate.equals(VOID.entities.asNode()) && object.isLiteral()) {
					if (subject.isBlank()) {
						classCounts.put(subject.getBlankNodeLabel(), parseLong(object));
					}
				} else if (predicate.equals(EVOID.specialClass.asNode())) {
					specialClasses.put(subject.getBlankNodeLabel(), object.isBlank() ? object.getBlankNodeLabel() : object.getURI());
				} else if (predicate.equals(EVOID.entities.asNode()) && object.isLiteral()) {
					specialClassesCounts.put(subject.getBlankNodeLabel(), parseLong(object));
				} else if (predicate.equals(VOID.property.asNode())) {
					properties.put(subject.getBlankNodeLabel(), object.isBlank() ? object.getBlankNodeLabel() : object.getURI());
				} else if (predicate.equals(VOID.triples.asNode()) && object.isLiteral()) {
					if (subject.isURI()) {
						if (datasetDescriptions.containsKey(subject.getURI())) {
							datasetDescriptions.get(subject.getURI()).triples = parseLong(object);
						}
					} else {
						propertyCounts.put(subject.getBlankNodeLabel(), parseLong(object));
					}
				} else if (predicate.equals(VOID.vocabulary.asNode())) {
					vocabularies.add(object.toString());
				} else if (predicate.equals(DCTerms.title.asNode())) {
					String datasetUri = subject.getURI();
					DatasetDescription description;
					if (!datasetDescriptions.containsKey(datasetUri)) {
						description = new DatasetDescription(datasetUri);
						datasetDescriptions.put(datasetUri, description);
					} else {
						description = datasetDescriptions.get(datasetUri);
					}
					description.title = object.toString();
				} else if (predicate.equals(VOID.subset.asNode())) {
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
				} else if (predicate.equals(DCTerms.description.asNode())) {
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

		public void addVoidToDocument(Document document) {
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

		protected long parseLong(Node object) {
			Object value = object.getLiteralValue();
			if (value instanceof Integer) {
				return (Integer) value;
			} else if (value instanceof Long) {
				return (Long) value;
			} else {
				LOGGER.error("Got an unknown literal type \"" + value.getClass().toString() + "\".");
			}
			return 0;
		}
	}
}
