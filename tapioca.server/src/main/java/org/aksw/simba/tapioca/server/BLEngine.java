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

import org.aksw.simba.tapioca.data.Dataset;
import org.aksw.simba.tapioca.data.StringCountMapping;
import org.aksw.simba.tapioca.gen.data.DatasetURIs;
import org.aksw.simba.tapioca.preprocessing.JenaBasedVoidParsingSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.tapioca.server.data.SimpleVector;
import org.aksw.simba.topicmodeling.commons.collections.TopDoubleObjectCollection;
import org.aksw.simba.topicmodeling.io.gzip.GZipCorpusObjectReader;
import org.aksw.simba.topicmodeling.preprocessing.SingleDocumentPreprocessor;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentDescription;
import org.aksw.simba.topicmodeling.utils.doc.DocumentName;
import org.aksw.simba.topicmodeling.utils.doc.DocumentText;
import org.aksw.simba.topicmodeling.utils.doc.DocumentURI;
import org.aksw.simba.topicmodeling.utils.doc.StringContainingDocumentProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectOpenHashSet;

public class BLEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(BLEngine.class);

    private static final int DEFAULT_NUMBER_OF_RESULTS = 20;
    public static final String CORPUS_FILE_NAME = "lodStats_BL_final.corpus";

    public static BLEngine createEngine(File inputFolder) {
        GZipCorpusObjectReader corpusReader = new GZipCorpusObjectReader(new File(inputFolder.getAbsolutePath()
                + File.separator + CORPUS_FILE_NAME));
        Corpus corpus = corpusReader.getCorpus();
        if (corpus == null) {
            LOGGER.error("Couldn't read corpus. Returning null.");
            return null;
        }
        ObjectObjectOpenHashMap<Dataset, ObjectOpenHashSet<String>> knownDatasets = new ObjectObjectOpenHashMap<Dataset, ObjectOpenHashSet<String>>(
                corpus.getNumberOfDocuments());
        // generate a URI set for each document
        DatasetURIs uris;
        for (Document document : corpus) {
            uris = document.getProperty(DatasetURIs.class);
            if (uris == null) {
                LOGGER.warn("Got a document without DatasetURIs property. Ignoring this document.");
            } else {
                knownDatasets.put(createDataset(document), uris.get());
            }
        }
        SingleDocumentPreprocessor preprocessor = createPreprocessing();
        if (preprocessor == null) {
            LOGGER.error("Couldn't create preprocessor. Returning null.");
            return null;
        }
        return new BLEngine(knownDatasets, preprocessor);
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

    protected static SingleDocumentPreprocessor createPreprocessing() {
        SingleDocumentPreprocessor preprocessor = new SingleDocumentPreprocessor();
        DocumentSupplier supplier = preprocessor;
        // parse VOID
        supplier = new JenaBasedVoidParsingSupplierDecorator(supplier);

        // Count the URIs
        supplier = new UriCountMappingCreatingDocumentSupplierDecorator(supplier, UriUsage.CLASSES_AND_PROPERTIES);

        preprocessor.setDocumentSupplier(supplier);
        return preprocessor;
    }

    private ObjectObjectOpenHashMap<Dataset, ObjectOpenHashSet<String>> knownDatasets;
    private SingleDocumentPreprocessor preprocessor;
    private int numberOfResults = DEFAULT_NUMBER_OF_RESULTS;

    protected BLEngine(ObjectObjectOpenHashMap<Dataset, ObjectOpenHashSet<String>> knownDatasets,
            SingleDocumentPreprocessor preprocessor) {
        this.knownDatasets = knownDatasets;
        this.preprocessor = preprocessor;
    }

    public TopDoubleObjectCollection<Dataset> retrieveSimilarDatasets(String voidString) {
        // preprocess given void string
        LOGGER.info("Preprocessing void string...");
        Document document = preprocess(voidString);
        if (document == null) {
            throw new IllegalArgumentException("The given void string did not result in a valid document.");
        }
        // retrieve the most similar datasets
        LOGGER.info("Retrieving similar datasets...");
        TopDoubleObjectCollection<Dataset> result = retrieveMostSimilarDataset(document
                .getProperty(StringCountMapping.class));
        // return a sorted list
        LOGGER.info("Done.");
        return result;
    }

    @SuppressWarnings("unchecked")
    private TopDoubleObjectCollection<Dataset> retrieveMostSimilarDataset(StringCountMapping uriCounts) {
        TopDoubleObjectCollection<Dataset> results = new TopDoubleObjectCollection<Dataset>(numberOfResults, false);
        ObjectOpenHashSet<String> documentURIs = new ObjectOpenHashSet<String>(uriCounts.get().keys());
        // simply go over all known datasets and calculate the similarities
        for (int i = 0; i < knownDatasets.allocated.length; ++i) {
            if (knownDatasets.allocated[i]) {
                results.add(
                        getSimilarity(documentURIs, (ObjectOpenHashSet<String>) ((Object[]) knownDatasets.values)[i]),
                        (Dataset) ((Object[]) knownDatasets.keys)[i]);
            }
        }
        return results;
    }

    private double getSimilarity(ObjectOpenHashSet<String> documentURIs, ObjectOpenHashSet<String> knownDataset) {
        int overlapCount = 0;
        for (int i = 0; i < documentURIs.allocated.length; ++i) {
            if (documentURIs.allocated[i]) {
                if (knownDataset.contains((String) ((Object[]) documentURIs.keys)[i])) {
                    ++overlapCount;
                }
            }
        }
        double unionCount = documentURIs.assigned + knownDataset.assigned - overlapCount;
        return overlapCount / unionCount;
    }

    private synchronized Document preprocess(String voidString) {
        Document document = new Document();
        document.addProperty(new DocumentText(voidString));
        return preprocessor.processDocument(document);
    }
}
