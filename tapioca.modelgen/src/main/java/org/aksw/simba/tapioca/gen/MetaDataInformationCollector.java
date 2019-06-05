/**
 * tapioca.modelgen - ${project.description}
 * Copyright © 2015 Data Science Group (DICE) (michael.roeder@uni-paderborn.de)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * This file is part of tapioca.modelgen.
 *
 * tapioca.modelgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.modelgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.modelgen.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.gen;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.aksw.simba.tapioca.extraction.AbstractExtractor;
import org.aksw.simba.tapioca.extraction.RDF2ExtractionStreamer;
import org.aksw.simba.tapioca.extraction.voidex.DatasetDescription;
import org.aksw.simba.tapioca.gen.data.StatResult;
import org.aksw.simba.tapioca.gen.preprocessing.StatResultsReader;
import org.dice_research.topicmodeling.io.CorpusReader;
import org.dice_research.topicmodeling.io.gzip.GZipCorpusObjectReader;
import org.dice_research.topicmodeling.io.gzip.GZipCorpusObjectWriter;
import org.dice_research.topicmodeling.preprocessing.ListCorpusCreator;
import org.dice_research.topicmodeling.preprocessing.docsupplier.CorpusWrappingDocumentSupplier;
import org.dice_research.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.AbstractDocumentSupplierDecorator;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.DocumentFilteringSupplierDecorator;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.filter.DocumentFilter;
import org.dice_research.topicmodeling.utils.corpus.Corpus;
import org.dice_research.topicmodeling.utils.corpus.DocumentListCorpus;
import org.dice_research.topicmodeling.utils.doc.Document;
import org.dice_research.topicmodeling.utils.doc.DocumentDescription;
import org.dice_research.topicmodeling.utils.doc.DocumentName;
import org.dice_research.topicmodeling.utils.doc.DocumentURI;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.n3.turtle.TurtleReader;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.RDFReader;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.DC;
import org.apache.jena.vocabulary.OWL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectIntOpenHashMap;

public class MetaDataInformationCollector {

	private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataInformationCollector.class);

	private static final String LOD_STATS_DOC_BASE_URI = "http://lodstats.aksw.org/rdfdocs/";

	// private static final String LOD_STATS_DATASET_TITLE =
	// "http://lodstats.aksw.org/ontology/extras/catalog-name";

	public static void main(String[] args) {
		MetaDataInformationCollector collector = new MetaDataInformationCollector();
		collector.run("/Daten/Dropbox/lodstats-rdf/23032015/datasets.nt", "/Daten/tapioca/lodStats_BL.object",
				"/Daten/Dropbox/lodstats-rdf/23032015/statresult.nt", "/Daten/tapioca/test.corpus",
				"/Daten/tapioca/lodStats_model/lodstats.nt");
	}

	// PipedRDFStream and PipedRDFIterator need to be on different threads
	private RDF2ExtractionStreamer streamer = new RDF2ExtractionStreamer();

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
		LODStatsMetaDataExtractor extractor = new LODStatsMetaDataExtractor();
		FileInputStream fin = null;
		try {
			fin = new FileInputStream(metaFileName);
			streamer.runExtraction(fin, LOD_STATS_DOC_BASE_URI, Lang.TTL, extractor);
		} catch (Exception e) {
			LOGGER.error("Error while parsing file \"" + metaFileName + "\". Aborting.", e);
			return null;
		} finally {
			IOUtils.closeQuietly(fin);
		}
		return extractor.descriptions;
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
		Statement s;
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

	protected static class LODStatsMetaDataExtractor extends AbstractExtractor {

		public static final String DCAT_ACCESS_URL_URI = "http://www.w3.org/ns/dcat#accessURL";

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
