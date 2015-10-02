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
import org.aksw.simba.topicmodeling.utils.doc.DocumentURI;
import org.aksw.simba.topicmodeling.utils.doc.StringContainingDocumentProperty;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.hp.hpl.jena.n3.turtle.TurtleReader;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFReader;

public class BLEngine extends AbstractEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(BLEngine.class);

    private static final int DEFAULT_NUMBER_OF_RESULTS = 20;
    public static final String CORPUS_FILE_NAME = "lodStats_BL_final.corpus";

    public static BLEngine createEngine(File inputFolder, File metaDataFile) {
        GZipCorpusObjectReader corpusReader = new GZipCorpusObjectReader(new File(inputFolder.getAbsolutePath()
                + File.separator + CORPUS_FILE_NAME));
        Corpus corpus = corpusReader.getCorpus();
        if (corpus == null) {
            LOGGER.error("Couldn't read corpus. Returning null.");
            return null;
        }
        ObjectObjectOpenHashMap<String, ObjectOpenHashSet<String>> knownDatasets = new ObjectObjectOpenHashMap<String, ObjectOpenHashSet<String>>(
                corpus.getNumberOfDocuments());
        // generate a URI set for each document
        DatasetURIs uris;
        for (Document document : corpus) {
            uris = document.getProperty(DatasetURIs.class);
            if (uris == null) {
                LOGGER.warn("Got a document without DatasetURIs property. Ignoring this document.");
            } else {
                knownDatasets.put(getUri(document), uris.get());
            }
        }
        SingleDocumentPreprocessor preprocessor = createPreprocessing();
        if (preprocessor == null) {
            LOGGER.error("Couldn't create preprocessor. Returning null.");
            return null;
        }
        // Read additional meta data
        RDFReader reader = new TurtleReader();
        Model metaDataModel = ModelFactory.createDefaultModel();
        FileInputStream fin = null;
        try {
            fin = new FileInputStream(metaDataFile);
            reader.read(metaDataModel, fin, "");
        } catch (FileNotFoundException e) {
            LOGGER.error("Couldn't read meta data from file. Returning null.", e);
            return null;
        } finally {
            IOUtils.closeQuietly(fin);
        }
        
        return new BLEngine(knownDatasets, preprocessor, metaDataModel);
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

    private ObjectObjectOpenHashMap<String, ObjectOpenHashSet<String>> knownDatasets;
    private int numberOfResults = DEFAULT_NUMBER_OF_RESULTS;

    protected BLEngine(ObjectObjectOpenHashMap<String, ObjectOpenHashSet<String>> knownDatasets,
            SingleDocumentPreprocessor preprocessor, Model rdfMetaDataModel) {
        super(preprocessor, rdfMetaDataModel);
        this.knownDatasets = knownDatasets;
    }

    @Override
    protected TopDoubleObjectCollection<String> retrieveSimilarDatasets(Document document) {
        // retrieve the most similar datasets
        LOGGER.info("Retrieving similar datasets...");
        TopDoubleObjectCollection<String> result = retrieveMostSimilarDataset(document
                .getProperty(StringCountMapping.class));
        // return a sorted list
        LOGGER.info("Done.");
        return result;
    }

    @SuppressWarnings("unchecked")
    private TopDoubleObjectCollection<String> retrieveMostSimilarDataset(StringCountMapping uriCounts) {
        TopDoubleObjectCollection<String> results = new TopDoubleObjectCollection<String>(numberOfResults, false);
        ObjectOpenHashSet<String> documentURIs = new ObjectOpenHashSet<String>(uriCounts.get().keys());
        // simply go over all known datasets and calculate the similarities
        for (int i = 0; i < knownDatasets.allocated.length; ++i) {
            if (knownDatasets.allocated[i]) {
                results.add(
                        getSimilarity(documentURIs, (ObjectOpenHashSet<String>) ((Object[]) knownDatasets.values)[i]),
                        (String) ((Object[]) knownDatasets.keys)[i]);
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
}
