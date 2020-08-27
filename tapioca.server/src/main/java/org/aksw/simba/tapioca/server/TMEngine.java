/**
 * tapioca.server - ${project.description}
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
 * This file is part of tapioca.server.
 *
 * tapioca.server is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.server is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.server.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Set;

import org.aksw.simba.tapioca.data.Dataset;
import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.data.DatasetSpecialClassesInfo;
import org.aksw.simba.tapioca.data.SimpleTokenizedText;
import org.aksw.simba.tapioca.data.VocabularyBlacklist;
import org.aksw.simba.tapioca.preprocessing.JenaBasedVoidParsingSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.SimpleBlankNodeRemovingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.SimpleTokenizedTextTermFilter;
import org.aksw.simba.tapioca.preprocessing.SimpleWordIndexingSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.WordOccurence;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.tapioca.preprocessing.UriFilteringDocumentSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.server.data.SimpleVector;
import org.aksw.simba.tapioca.server.similarity.CosineSimilarity;
import org.aksw.simba.tapioca.server.similarity.VectorSimilarity;
import org.apache.commons.io.IOUtils;
import org.apache.jena.n3.turtle.TurtleReader;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFReader;
import org.dice_research.topicmodeling.algorithms.ModelingAlgorithm;
import org.dice_research.topicmodeling.algorithms.ProbTopicModelingAlgorithmStateSupplier;
import org.dice_research.topicmodeling.algorithms.ProbabilisticWordTopicModel;
import org.dice_research.topicmodeling.commons.collections.TopDoubleObjectCollection;
import org.dice_research.topicmodeling.io.CorpusReader;
import org.dice_research.topicmodeling.io.gzip.GZipCorpusReaderDecorator;
import org.dice_research.topicmodeling.io.gzip.GZipProbTopicModelingAlgorithmStateReader;
import org.dice_research.topicmodeling.io.java.CorpusObjectReader;
import org.dice_research.topicmodeling.lang.postagging.StandardEnglishPosTaggingTermFilter;
import org.dice_research.topicmodeling.preprocessing.SingleDocumentPreprocessor;
import org.dice_research.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.DocumentFilteringSupplierDecorator;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.DocumentWordCountingSupplierDecorator;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.filter.DocumentFilter;
import org.dice_research.topicmodeling.utils.corpus.Corpus;
import org.dice_research.topicmodeling.utils.doc.Document;
import org.dice_research.topicmodeling.utils.doc.DocumentDescription;
import org.dice_research.topicmodeling.utils.doc.DocumentName;
import org.dice_research.topicmodeling.utils.doc.DocumentURI;
import org.dice_research.topicmodeling.utils.doc.ProbabilisticClassificationResult;
import org.dice_research.topicmodeling.utils.doc.StringContainingDocumentProperty;
import org.dice_research.topicmodeling.utils.vocabulary.Vocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;

public class TMEngine extends AbstractEngine {

	private static final Logger LOGGER = LoggerFactory.getLogger(TMEngine.class);

	private static final int DEFAULT_NUMBER_OF_RESULTS = 20;
	public static final String MODEL_FILE_NAME = "probAlgState.object";
	public static final String CORPUS_FILE_NAME = "lodStats_final.corpus";

	public static TMEngine createEngine(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever,
			File inputFolder, File metaDataFile) {
		CorpusReader reader = new GZipCorpusReaderDecorator(new CorpusObjectReader());
		reader.readCorpus(new File(inputFolder.getAbsolutePath() + File.separator + CORPUS_FILE_NAME));
		Corpus corpus = reader.getCorpus();
		if (corpus == null) {
			LOGGER.error("Couldn't read corpus. Returning null.");
			return null;
		}
		return createEngine(cachingLabelRetriever, corpus, inputFolder, metaDataFile);
	}

	public static TMEngine createEngine(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever,
			Corpus corpus, File inputFolder, File metaDataFile) {
		LOGGER.info("Loading model from \"" + inputFolder.getAbsolutePath() + "\".");
		// read probabilistic word topic Model from file
		GZipProbTopicModelingAlgorithmStateReader modelReader = new GZipProbTopicModelingAlgorithmStateReader();
		ProbTopicModelingAlgorithmStateSupplier model = (ProbTopicModelingAlgorithmStateSupplier) modelReader
				.readProbTopicModelState(new File(inputFolder.getAbsolutePath() + File.separator + MODEL_FILE_NAME));
		if (model == null) {
			LOGGER.error("Couldn't read model. Returning null.");
			return null;
		}
		ProbabilisticWordTopicModel probModel = (ProbabilisticWordTopicModel) ((ModelingAlgorithm) model).getModel();

		ObjectObjectOpenHashMap<String, SimpleVector> knownDatasets = new ObjectObjectOpenHashMap<String, SimpleVector>(
				corpus.getNumberOfDocuments());
		// translate word topic assignment into topic vectors for each document
		SingleDocumentPreprocessor tempPreProc = new SingleDocumentPreprocessor();
		DocumentWordCountingSupplierDecorator decorator = new DocumentWordCountingSupplierDecorator(tempPreProc);
		tempPreProc.setDocumentSupplier(decorator);
		for (int i = 0; i < corpus.getNumberOfDocuments(); ++i) {
			// knownDatasets.put(createDataset(corpus.getDocument(i)),
			// createVector(model.getWordTopicAssignmentForDocument(i),
			// model.getNumberOfTopics()));
			// let's use smoothing for this
			knownDatasets.put(getUri(corpus.getDocument(i)), new SimpleVector((double[]) probModel
					.getClassificationForDocument(tempPreProc.processDocument(corpus.getDocument(i))).getValue()));
		}
		SingleDocumentPreprocessor preprocessor = createPreprocessing(cachingLabelRetriever, model.getVocabulary());
		if (preprocessor == null) {
			LOGGER.error("Couldn't create preprocessor. Returning null.");
			return null;
		}
		// Read additional meta data
		RDFReader reader = new TurtleReader();
		Model metaDataModel = ModelFactory.createDefaultModel();
		FileInputStream fin = null;
		if (metaDataFile != null) {
			try {
				fin = new FileInputStream(metaDataFile);
				reader.read(metaDataModel, fin, "");
			} catch (FileNotFoundException e) {
				LOGGER.error("Couldn't read meta data from file. Returning null.", e);
				return null;
			} finally {
				IOUtils.closeQuietly(fin);
			}
		}

		return new TMEngine(probModel, knownDatasets, preprocessor, metaDataModel);
	}

	protected static String getUri(Document document) {
		DocumentURI docUri = document.getProperty(DocumentURI.class);
		if (docUri != null) {
			return docUri.get();
		} else {
			LOGGER.error("Got a document without the needed DocumentURI property. Ignoring this document.");
		}
		return null;
	}

	protected static Dataset createDataset(Document document) {
		StringContainingDocumentProperty property;
		String name = null, uri = null, description = null;
		property = document.getProperty(DocumentName.class);
		if (property != null) {
			name = property.getStringValue();
		}
		property = document.getProperty(DocumentURI.class);
		if (property != null) {
			uri = property.getStringValue();
		}
		property = document.getProperty(DocumentDescription.class);
		if (property != null) {
			description = property.getStringValue();
		}
		return new Dataset(name, uri, description);
	}

	protected static SimpleVector createVector(int[] wordTopicAssignments, int numberOfTopics) {
		double vector[] = new double[numberOfTopics];
		for (int i = 0; i < wordTopicAssignments.length; ++i) {
			++vector[wordTopicAssignments[i]];
		}
		return new SimpleVector(vector);
	}

	protected static SingleDocumentPreprocessor createPreprocessing(
			WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever, Vocabulary vocabulary) {
		SingleDocumentPreprocessor preprocessor = new SingleDocumentPreprocessor();
		DocumentSupplier supplier = preprocessor;
		// parse VOID
		supplier = new JenaBasedVoidParsingSupplierDecorator(supplier);

		// Filter URIs
		Set<String> blacklist = VocabularyBlacklist.getInstance();
		supplier = new UriFilteringDocumentSupplierDecorator<DatasetClassInfo>(supplier, blacklist,
				DatasetClassInfo.class);
		supplier = new SimpleBlankNodeRemovingDocumentSupplierDecorator<DatasetClassInfo>(supplier,
				DatasetClassInfo.class);
		supplier = new UriFilteringDocumentSupplierDecorator<DatasetPropertyInfo>(supplier, blacklist,
				DatasetPropertyInfo.class);
		supplier = new SimpleBlankNodeRemovingDocumentSupplierDecorator<DatasetPropertyInfo>(supplier,
				DatasetPropertyInfo.class);
		supplier = new SimpleBlankNodeRemovingDocumentSupplierDecorator<DatasetSpecialClassesInfo>(supplier,
				DatasetSpecialClassesInfo.class);

		// Count the URIs
		supplier = new UriCountMappingCreatingDocumentSupplierDecorator(supplier, UriUsage.CLASSES_AND_PROPERTIES);

		// retrieve labels
		// Check whether there is a file containing labels
		cachingLabelRetriever.setDecoratedDocumentSupplier(supplier);
		supplier = cachingLabelRetriever;
		// supplier = new ExceptionCatchingDocumentSupplierDecorator(supplier);
		// Convert the counted tokens into tokenized text
		supplier = new StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator(supplier, WordOccurence.LOG);
		// Filter the stop words
		supplier = new SimpleTokenizedTextTermFilter(supplier, StandardEnglishPosTaggingTermFilter.getInstance());
		// Filter empty documents
		supplier = new DocumentFilteringSupplierDecorator(supplier, new DocumentFilter() {
			public boolean isDocumentGood(Document document) {
				SimpleTokenizedText text = document.getProperty(SimpleTokenizedText.class);
				return (text != null) && (text.getTokens().length > 0);
			}
		});
		supplier = new SimpleWordIndexingSupplierDecorator(supplier, vocabulary);
		// Create DocumentWordCounts
		supplier = new DocumentWordCountingSupplierDecorator(supplier);

		preprocessor.setDocumentSupplier(supplier);
		return preprocessor;
	}

	private ProbabilisticWordTopicModel model;
	private ObjectObjectOpenHashMap<String, SimpleVector> knownDatasets;
	private VectorSimilarity similarity;
	private int numberOfResults = DEFAULT_NUMBER_OF_RESULTS;

	protected TMEngine(ProbabilisticWordTopicModel model, ObjectObjectOpenHashMap<String, SimpleVector> knownDatasets,
			SingleDocumentPreprocessor preprocessor, Model rdfMetaDataModel) {
		this(model, knownDatasets, preprocessor, rdfMetaDataModel, new CosineSimilarity());
	}

	protected TMEngine(ProbabilisticWordTopicModel model, ObjectObjectOpenHashMap<String, SimpleVector> knownDatasets,
			SingleDocumentPreprocessor preprocessor, Model rdfMetaDataModel, VectorSimilarity similarity) {
		super(preprocessor, rdfMetaDataModel);
		this.knownDatasets = knownDatasets;
		this.model = model;
		this.similarity = similarity;
	}

	@Override
	protected TopDoubleObjectCollection<String> retrieveSimilarDatasets(Document document) {
		// infer topic vector
		// TODO check whether the inference is thread-safe
		LOGGER.info("Infering topics...");
		ProbabilisticClassificationResult classification = (ProbabilisticClassificationResult) model
				.getClassificationForDocument(document);
		// retrieve the most similar datasets
		LOGGER.info("Retrieving similar datasets...");
		TopDoubleObjectCollection<String> result = retrieveMostSimilarDataset(
				new SimpleVector(classification.getTopicProbabilities()));
		// return a sorted list
		LOGGER.info("Done.");
		return result;
	}

	private TopDoubleObjectCollection<String> retrieveMostSimilarDataset(SimpleVector vector) {
		TopDoubleObjectCollection<String> results = new TopDoubleObjectCollection<String>(numberOfResults, false);
		// simply go over all known datasets and calculate the similarities
		for (int i = 0; i < knownDatasets.allocated.length; ++i) {
			if (knownDatasets.allocated[i]) {
				results.add(similarity.getSimilarity(vector, (SimpleVector) ((Object[]) knownDatasets.values)[i]),
						(String) ((Object[]) knownDatasets.keys)[i]);
			}
		}
		return results;
	}

	public void setNumberOfResults(int numberOfResults) {
		this.numberOfResults = numberOfResults;
	}
}
