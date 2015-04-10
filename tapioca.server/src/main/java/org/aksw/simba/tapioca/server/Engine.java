/**
 * The MIT License
 * Copyright (c) 2015 Michael Röder (roeder@informatik.uni-leipzig.de)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.aksw.simba.tapioca.server;

import java.io.File;
import java.util.Set;

import org.aksw.simba.tapioca.data.Dataset;
import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.data.DatasetSpecialClassesInfo;
import org.aksw.simba.tapioca.data.VocabularyBlacklist;
import org.aksw.simba.tapioca.preprocessing.JenaBasedVoidParsingSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.SimpleBlankNodeRemovingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.tapioca.preprocessing.UriFilteringDocumentSupplierDecorator;
import org.aksw.simba.tapioca.server.data.SimpleVector;
import org.aksw.simba.tapioca.server.similarity.CosineSimilarity;
import org.aksw.simba.tapioca.server.similarity.VectorSimilarity;
import org.aksw.simba.topicmodeling.algorithms.ProbTopicModelingAlgorithmStateSupplier;
import org.aksw.simba.topicmodeling.algorithms.ProbabilisticWordTopicModel;
import org.aksw.simba.topicmodeling.commons.collections.TopDoubleObjectCollection;
import org.aksw.simba.topicmodeling.io.gzip.GZipCorpusObjectReader;
import org.aksw.simba.topicmodeling.io.gzip.GZipProbTopicModelingAlgorithmStateReader;
import org.aksw.simba.topicmodeling.preprocessing.SingleDocumentPreprocessor;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentText;
import org.aksw.simba.topicmodeling.utils.doc.ProbabilisticClassificationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;

public class Engine {

    private static final Logger LOGGER = LoggerFactory.getLogger(Engine.class);

    private static final int DEFAULT_NUMBER_OF_RESULTS = 20;
    public static final String MODEL_FILE_NAME = "model.object";
    public static final String CORPUS_FILE_NAME = "corpus.object";

    public static Engine createEngine(File inputFolder) {
        LOGGER.info("Loading model from \"" + inputFolder.getAbsolutePath() + "\".");
        // read probabilistic word topic Model from file
        GZipProbTopicModelingAlgorithmStateReader modelReader = new GZipProbTopicModelingAlgorithmStateReader();
        ProbTopicModelingAlgorithmStateSupplier model = (ProbTopicModelingAlgorithmStateSupplier) modelReader
                .readProbTopicModelState(new File(inputFolder.getAbsolutePath() + File.separator + MODEL_FILE_NAME));
        if (model == null) {
            LOGGER.error("Couldn't read model. Returning null.");
            return null;
        }
        GZipCorpusObjectReader corpusReader = new GZipCorpusObjectReader(new File(inputFolder.getAbsolutePath()
                + File.separator + CORPUS_FILE_NAME));
        Corpus corpus = corpusReader.getCorpus();
        if (corpus == null) {
            LOGGER.error("Couldn't read corpus. Returning null.");
            return null;
        }
        ObjectObjectOpenHashMap<Dataset, SimpleVector> knownDatasets = new ObjectObjectOpenHashMap<Dataset, SimpleVector>(
                corpus.getNumberOfDocuments());
        // translate word topic assignment into topic vectors for each document
        for (int i = 0; i < corpus.getNumberOfDocuments(); ++i) {
            knownDatasets.put(createDataset(corpus.getDocument(i)),
                    createVector(model.getWordTopicAssignmentForDocument(i), model.getNumberOfTopics()));
        }
        SingleDocumentPreprocessor preprocessor = createPreprocessing();
        if (preprocessor == null) {
            LOGGER.error("Couldn't create preprocessor. Returning null.");
            return null;
        }
        return new Engine((ProbabilisticWordTopicModel) model, knownDatasets, preprocessor);
    }

    private static Dataset createDataset(Document document) {
        // TODO
        return null;
    }

    private static SimpleVector createVector(int[] wordTopicAssignments, int numberOfTopics) {
        double vector[] = new double[numberOfTopics];
        for (int i = 0; i < wordTopicAssignments.length; ++i) {
            ++vector[wordTopicAssignments[i]];
        }
        return new SimpleVector(vector);
    }

    private static SingleDocumentPreprocessor createPreprocessing() {
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
        //
        return null;
    }

    private ProbabilisticWordTopicModel model;
    private ObjectObjectOpenHashMap<Dataset, SimpleVector> knownDatasets;
    private SingleDocumentPreprocessor preprocessor;
    private VectorSimilarity similarity;
    private int numberOfResults = DEFAULT_NUMBER_OF_RESULTS;

    protected Engine(ProbabilisticWordTopicModel model, ObjectObjectOpenHashMap<Dataset, SimpleVector> knownDatasets,
            SingleDocumentPreprocessor preprocessor) {
        this(model, knownDatasets, preprocessor, new CosineSimilarity());
    }

    protected Engine(ProbabilisticWordTopicModel model, ObjectObjectOpenHashMap<Dataset, SimpleVector> knownDatasets,
            SingleDocumentPreprocessor preprocessor, VectorSimilarity similarity) {
        this.knownDatasets = knownDatasets;
        this.model = model;
        this.preprocessor = preprocessor;
        this.similarity = similarity;
    }

    public TopDoubleObjectCollection<Dataset> retrieveSimilarDatasets(String voidString) {
        // preprocess given void string
        Document document = preprocess(voidString);
        // infer topic vector
        // TODO check whether the inference is thread-safe
        ProbabilisticClassificationResult classification = (ProbabilisticClassificationResult) model
                .getClassificationForDocument(document);
        // retrieve the most similar datasets
        TopDoubleObjectCollection<Dataset> result = retrieveMostSimilarDataset(new SimpleVector(
                classification.getTopicProbabilities()));
        // return a sorted list
        return result;
    }

    private TopDoubleObjectCollection<Dataset> retrieveMostSimilarDataset(SimpleVector vector) {
        TopDoubleObjectCollection<Dataset> results = new TopDoubleObjectCollection<Dataset>(numberOfResults, false);
        // simply go over all known datasets and calculate the similarities
        for (int i = 0; i < knownDatasets.allocated.length; ++i) {
            if (knownDatasets.allocated[i]) {
                results.add(similarity.getSimilarity(vector, (SimpleVector) ((Object[]) knownDatasets.values)[i]),
                        (Dataset) ((Object[]) knownDatasets.keys)[i]);
            }
        }
        return results;
    }

    private synchronized Document preprocess(String voidString) {
        Document document = new Document();
        document.addProperty(new DocumentText(voidString));
        return preprocessor.processDocument(document);
    }
}
