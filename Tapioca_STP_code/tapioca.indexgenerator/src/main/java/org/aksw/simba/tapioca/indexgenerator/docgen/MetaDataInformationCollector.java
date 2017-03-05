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
package org.aksw.simba.tapioca.indexgenerator.docgen;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.aksw.simba.tapioca.cores.data.DatasetDescription;
import org.aksw.simba.tapioca.cores.extraction.AbstractExtractor;
import org.aksw.simba.tapioca.cores.extraction.RDF2ExtractionStreamer;
import org.aksw.simba.tapioca.indexgenerator.docgen.data.StatResult;
import org.aksw.simba.tapioca.indexgenerator.docgen.preprocessing.StatResultsReader;
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
import org.apache.jena.riot.Lang;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
//import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.n3.turtle.TurtleReader;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.RDFReader;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.OWL;

/**
 * 
 * @author Michael Roeder, Marleen Wagner
 *
 */
public class MetaDataInformationCollector {

	// -------------------------------------------------------------------------
	// ------------------ Variables --------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Logger for errors, warnings and other informations.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataInformationCollector.class);

	/**
	 * The base URI for the LodStats file.
	 */
	private static final String LOD_STATS_DOC_BASE_URI = "http://lodstats.aksw.org/rdfdocs/";

	/**
	 * RDF extraction streamer.
	 */
	private RDF2ExtractionStreamer streamer = new RDF2ExtractionStreamer();

	// -------------------------------------------------------------------------
	// ------------------ Methods ----------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Run meta data information collection.
	 * 
	 * @param metaFileName
	 *            datasets.nt
	 * @param corpusFileName
	 *            LDA_corpus_file
	 * @param statResultsFile
	 *            statresults.nt
	 * @param corpusOutFileName
	 *            finalCorpusFile
	 * @param additionalMetaDataFile
	 *            lodstats.nt
	 * @throws FileNotFoundException
	 */
	public void run(String metaFileName, String corpusFileName, String statResultsFile, String corpusOutFileName,
			String additionalMetaDataFile) throws FileNotFoundException {

		StatResultsReader reader = new StatResultsReader();
		IntObjectOpenHashMap<StatResult> statResults = reader.read(statResultsFile);
		IntObjectOpenHashMap<DatasetDescription> descriptions = readDescriptions(metaFileName);

		if (additionalMetaDataFile != null) {
			// TODO filter documents
			enricheMetaData(additionalMetaDataFile, descriptions);
		}
		addDescriptionsToCorpus(descriptions, statResults, corpusFileName, corpusOutFileName);
	}

	/**
	 * Extract the corpus from a file.
	 * 
	 * @param corpusFileName
	 *            path to corpus file
	 * @return the corpus extracted from file
	 */
	protected Corpus readCorpus(String corpusFileName) {
		CorpusReader reader = new GZipCorpusObjectReader(new File(corpusFileName));
		reader.readCorpus();
		return reader.getCorpus();
	}

	/**
	 * Read descriptions from file.
	 * 
	 * @param metaFileName
	 *            datasets.nt
	 * @return parsed descriptions
	 */
	protected IntObjectOpenHashMap<DatasetDescription> readDescriptions(String metaFileName) {
		LODStatsMetaDataExtractor extractor = new LODStatsMetaDataExtractor();
		FileInputStream fin = null;
		try {
			fin = new FileInputStream(metaFileName);
			streamer.runExtraction(fin, LOD_STATS_DOC_BASE_URI, Lang.NT, extractor);
		} catch (Exception e) {
			LOGGER.error("Error while parsing file \"" + metaFileName + "\". Aborting.", e);
			return null;
		} finally {
			IOUtils.closeQuietly(fin);
		}
		return extractor.descriptions;
	}

	/**
	 * Enrich meta data via the additional meta data file
	 * 
	 * @param additionalMetaDataFile
	 *            lodstats.nt
	 * @param descriptions
	 *            descriptions that have been read in from datasets.nt
	 */
	protected void enricheMetaData(String additionalMetaDataFile,
			IntObjectOpenHashMap<DatasetDescription> descriptions) {
		// Read additional meta data
		RDFReader reader = new TurtleReader();
		Model model = ModelFactory.createDefaultModel();
		FileInputStream fin = null;
		try {
			fin = new FileInputStream(additionalMetaDataFile);
			reader.read(model, fin, LOD_STATS_DOC_BASE_URI);
			LOGGER.info("Found additional metadata. Processing...");
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

	/**
	 * Update description, set new data set URI.
	 * 
	 * @param description
	 *            Description of the data set.
	 * @param datasetResource
	 *            Resource of the data set.
	 * @param model
	 *            The VoID model.
	 */
	protected void updateDescription(DatasetDescription description, Resource datasetResource, Model model) {
		// We prefer the "real" data set URI
		if (!datasetResource.getURI().startsWith(LOD_STATS_DOC_BASE_URI)) {
			description.uri = datasetResource.getURI();
		}
	}

	/**
	 * Add meta data to corpus.
	 * 
	 * @param descriptions
	 *            IntObjectOpenHashMap<DatasetDescription> descriptions from
	 *            readDescriptions()
	 * @param statResults
	 *            IntObjectOpenHashMap<StatResult> statResults from
	 *            StatResultsReader.read()
	 * @param corpusFileName
	 *            existing corpus, e.g. LDAcorpus
	 * @param corpusOutFileName
	 *            final corpus file
	 */
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

		LOGGER.info("Writing final Corpus with descriptions added...");
		GZipCorpusObjectWriter writer = new GZipCorpusObjectWriter(new File(corpusOutFileName));
		writer.writeCorpus(corpus2);
	}

	// -------------------------------------------------------------------------
	// ------------------ Static classes ---------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Used for extracting meta data from additional meta data files.
	 */
	protected static class LODStatsMetaDataExtractor extends AbstractExtractor {

		/**
		 * access URI
		 */
		public static final String DCAT_ACCESS_URL_URI = "http://www.w3.org/ns/dcat#accessURL";

		/**
		 * the descriptions read in from file
		 */
		public IntObjectOpenHashMap<DatasetDescription> descriptions = new IntObjectOpenHashMap<DatasetDescription>();

		@Override
		public void handleTriple(Triple triple) {
			Node subject = triple.getSubject();
			if (!subject.isBlank()) {
				String subjUri = subject.getURI();
				if (subjUri.startsWith(LOD_STATS_DOC_BASE_URI)) {
					int datasetId = getDatasetIdFromUri(subjUri);
					if (datasetId >= 0) {
						DatasetDescription description;
						if (descriptions.containsKey(datasetId)) {
							description = descriptions.get(datasetId);
						} else {
							description = new DatasetDescription(subjUri);
							descriptions.put(datasetId, description);
						}
						if (triple.getPredicate().getURI().equals(DCAT_ACCESS_URL_URI)) {
							description.title = triple.getObject().toString();
						} else if (triple.getPredicate().equals(DC.source.asNode())) {
							description.description = "Accessed through " + triple.getObject().toString();
						}
					}
				}
			}
		}

		/**
		 * gets datasedId from URI, same as name of file
		 * 
		 * @param uri
		 * @return
		 */
		protected static int getDatasetIdFromUri(String uri) {
			try {
				return Integer.parseInt(uri.substring(LOD_STATS_DOC_BASE_URI.length()));
			} catch (NumberFormatException e) {
				LOGGER.error("Couldn't extract the dataset id from URI \"" + uri + "\". Returning -1.", e);
				return -1;
			}
		}
	}

	/**
	 * A filter for the StatResult file.
	 *
	 */
	protected static class StatResultListBasedDocumentFilter implements DocumentFilter {

		private IntObjectOpenHashMap<StatResult> statResults;

		public StatResultListBasedDocumentFilter(IntObjectOpenHashMap<StatResult> statResults) {
			this.statResults = statResults;
		}

		/**
		 * checks if documentId is in statResults, so additional meta data can
		 * be loaded
		 */
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
		public Document getNextDocument() {
			Document document = documentSource.getNextDocument();
			if (document != null) {
				document = prepareDocument(document);
			}
			return document;
		}

		@Override
		protected Document prepareDocument(Document document) {
			DocumentName name = document.getProperty(DocumentName.class);
			int docId = getIdFromDocumentName(name);

			StatResult statResult = statResults.get(docId);
			int datasetId = LODStatsMetaDataExtractor.getDatasetIdFromUri(statResult.getDatasetUri());
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