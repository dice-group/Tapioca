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
import java.io.IOException;
import java.util.Set;

import org.aksw.simba.tapioca.server.TMEngine;

import org.aksw.simba.tapioca.cores.data.Dataset;
import org.aksw.simba.tapioca.cores.data.DatasetClassInfo;
import org.aksw.simba.tapioca.cores.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.cores.data.DatasetSpecialClassesInfo;
import org.aksw.simba.tapioca.cores.data.SimpleTokenizedText;
import org.aksw.simba.tapioca.cores.data.VocabularyBlacklist;
import org.aksw.simba.tapioca.cores.preprocessing.JenaBasedVoidParsingSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.SimpleBlankNodeRemovingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.SimpleTokenizedTextTermFilter;
import org.aksw.simba.tapioca.cores.preprocessing.SimpleWordIndexingSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.WordOccurence;
import org.aksw.simba.tapioca.cores.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.tapioca.cores.preprocessing.UriFilteringDocumentSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.server.data.SimpleVector;
import org.aksw.simba.tapioca.server.similarity.CosineSimilarity;
import org.aksw.simba.tapioca.server.similarity.VectorSimilarity;
import org.aksw.simba.topicmodeling.algorithms.ModelingAlgorithm;
import org.aksw.simba.topicmodeling.algorithms.ProbTopicModelingAlgorithmStateSupplier;
import org.aksw.simba.topicmodeling.algorithms.ProbabilisticWordTopicModel;
import org.aksw.simba.topicmodeling.commons.collections.TopDoubleObjectCollection;
import org.aksw.simba.topicmodeling.io.gzip.GZipCorpusObjectReader;
import org.aksw.simba.topicmodeling.io.gzip.GZipProbTopicModelingAlgorithmStateReader;
import org.aksw.simba.topicmodeling.lang.postagging.StandardEnglishPosTaggingTermFilter;
import org.aksw.simba.topicmodeling.preprocessing.SingleDocumentPreprocessor;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentFilteringSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentWordCountingSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.filter.DocumentFilter;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentDescription;
import org.aksw.simba.topicmodeling.utils.doc.DocumentName;
import org.aksw.simba.topicmodeling.utils.doc.DocumentText;
import org.aksw.simba.topicmodeling.utils.doc.DocumentURI;
import org.aksw.simba.topicmodeling.utils.doc.ProbabilisticClassificationResult;
import org.aksw.simba.topicmodeling.utils.doc.StringContainingDocumentProperty;
import org.aksw.simba.topicmodeling.utils.vocabulary.Vocabulary;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.hp.hpl.jena.rdf.model.Model;

public class TMEngine extends AbstractEngine {

	/**
	 * Logging
	 */
    private static final Logger LOGGER = LoggerFactory.getLogger(TMEngine.class);
    
    /**
     * Number of results
     */
    private static final int DEFAULT_NUMBER_OF_RESULTS = 20;
    
    /**
     * Number of results
     */
    private int numberOfResults = DEFAULT_NUMBER_OF_RESULTS;
        
    /**
     * Blacklist file name
     */
    public static final String BLACKLIST_FILE_NAME = "resources/vocabulary_blacklist.txt";       

	/**
	 * value of process bar
	 */
	public static Integer workProgress;		    
	
    /**
     * Model
     */
    private ProbabilisticWordTopicModel model;

    /**
     * Corpus
     */
    private static Corpus corpus;
    
    
    /**
     * Known datasets
     */
    private ObjectObjectOpenHashMap<String, SimpleVector> knownDatasets;
    
    /**
     * Vector similartiy method
     */
    private VectorSimilarity similarity;

    /**
     * Constructor
     * @param model
     * @param knownDatasets
     * @param preprocessor
     * @param rdfMetaDataModel
     */
    protected TMEngine(ProbabilisticWordTopicModel model, ObjectObjectOpenHashMap<String, SimpleVector> knownDatasets,
            SingleDocumentPreprocessor preprocessor, Model rdfMetaDataModel) {
        this(model, knownDatasets, preprocessor, rdfMetaDataModel, new CosineSimilarity());
		LOGGER.info("TMEngine created.");        
    }

    /**
     * Constructor
     * @param model
     * @param knownDatasets
     * @param preprocessor
     * @param rdfMetaDataModel
     * @param similarity
     */
    protected TMEngine(ProbabilisticWordTopicModel model, ObjectObjectOpenHashMap<String, SimpleVector> knownDatasets,
            SingleDocumentPreprocessor preprocessor, Model rdfMetaDataModel, VectorSimilarity similarity) {
        super(preprocessor, rdfMetaDataModel);
        this.knownDatasets = knownDatasets;
        this.model = model;
        this.similarity = similarity;
    }
  
    /**
     * Call the constructor
     * @param cachingLabelRetriever 
     * @param inputFolder 
     * @param metaDataFile 
     * @return TMEngine
     */
    public static TMEngine createEngine(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever,
            File modelfile, File corpusfile, Model rdfMetaDataModel) {
    	// read probabilistic word topic Model from file
    	LOGGER.info("Loading model from \"" + modelfile.getAbsolutePath() + "\".");
        GZipProbTopicModelingAlgorithmStateReader modelReader = new GZipProbTopicModelingAlgorithmStateReader();
        ProbTopicModelingAlgorithmStateSupplier model = 
        		(ProbTopicModelingAlgorithmStateSupplier) modelReader.readProbTopicModelState(
                        new File( modelfile.getAbsolutePath() ));
        if (model == null) {
            LOGGER.error("Couldn't read model. Returning null.");
            return null;
        }
        ProbabilisticWordTopicModel probModel = (ProbabilisticWordTopicModel) ((ModelingAlgorithm) model).getModel();
        
        // read corpus
        GZipCorpusObjectReader corpusReader = new GZipCorpusObjectReader(new File( corpusfile.getAbsolutePath() ) );
        corpus = corpusReader.getCorpus();
        if (corpus == null) {
            LOGGER.error("Couldn't read corpus. Returning null.");
            return null;            
        }
        
        // initialize known datasets
        ObjectObjectOpenHashMap<String, SimpleVector> knownDatasets = new ObjectObjectOpenHashMap<String, SimpleVector>(
                corpus.getNumberOfDocuments());
        
        // translate word topic assignment into topic vectors for each document
        SingleDocumentPreprocessor tempPreProc = new SingleDocumentPreprocessor();
        DocumentWordCountingSupplierDecorator decorator = new DocumentWordCountingSupplierDecorator(tempPreProc);
        tempPreProc.setDocumentSupplier(decorator);
        for (int i = 0; i < corpus.getNumberOfDocuments(); ++i) {
            knownDatasets.put(getUri(corpus.getDocument(i)), new SimpleVector((double[]) probModel
                    .getClassificationForDocument(tempPreProc.processDocument(corpus.getDocument(i))).getValue()));
        }
        
        // create preprocessor
        SingleDocumentPreprocessor preprocessor = createPreprocessing(cachingLabelRetriever, model.getVocabulary());
        if (preprocessor == null) {
            LOGGER.error("Couldn't create preprocessor. Returning null.");
            return null;
        }        
        // call constructor
        return new TMEngine(probModel, knownDatasets, preprocessor, rdfMetaDataModel);
    }

    /**
     * Get URI
     * @param document
     * @return URI
     */
    protected static String getUri(Document document) {
        DocumentURI docUri = document.getProperty(DocumentURI.class);
        if (docUri != null) {
            return docUri.get();
        } else {
            LOGGER.error("Got a document without the needed DocumentURI property. Ignoring this document.");
        }
        return null;
    }

    /**
     * Create dataset
     * @param document
     * @return Dataset
     */
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
    
    /**
     * Preprocess document
     * @param voidString RDF metadata input string
     * @return Document
     */
    public synchronized Document preprocess(String voidString) {
    	setWorkProgress(10);    	
        Document document = new Document();
        document.addProperty(new DocumentText(voidString));
        document = preprocessor.processDocument(document);
        return document;
    }    

    /**
     * Create sinmple vector
     * @param wordTopicAssignments
     * @param numberOfTopics
     * @return SimpleVector
     */
    protected static SimpleVector createVector(int[] wordTopicAssignments, int numberOfTopics) {
        double vector[] = new double[numberOfTopics];
        for (int i = 0; i < wordTopicAssignments.length; ++i) {
            ++vector[wordTopicAssignments[i]];
        }
        return new SimpleVector(vector);
    }

    /**
     * Create a preprocessor
     * @param cachingLabelRetriever
     * @param vocabulary Vocabulary
     * @return SingleDocumentPreprocessor
     */
    protected static SingleDocumentPreprocessor createPreprocessing(
            WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever, Vocabulary vocabulary) {
        SingleDocumentPreprocessor preprocessor = new SingleDocumentPreprocessor();
        DocumentSupplier supplier = preprocessor;
        // parse VOID
        supplier = new JenaBasedVoidParsingSupplierDecorator(supplier);

        // Filter URIs
        Set<String> blacklist = VocabularyBlacklist.getInstance(
        		TMEngine.class.getClassLoader().getResourceAsStream( BLACKLIST_FILE_NAME ));
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

	/**
	 * Retrieve similar datasets
	 * 
	 * @param document
	 * @return
	 */
	public TopDoubleObjectCollection<String> retrieveSimilarDatasets(Document document) {
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
    
	/**
	 * Retrieve similar datasets
	 * 
	 * @param vector
	 * @return
	 */
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
    
    /**
     * Get the corpus
     * @return corpus
     */    
    public Corpus getCorpus(){
    	return corpus;
    }

    /**
     * Get the model
     * @return model
     */    
    public ProbabilisticWordTopicModel getModel(){
    	return model;
    }

	/**
	 * Get value of progress bar
	 */
	public Integer getWorkProgress() {
		return workProgress;
	}

	/**
	 * Set value of progress bar
	 */
    public void setWorkProgress(Integer value) { 
        workProgress = value;  
    }  
    
	@Override
	public TransportClient getTransportClient() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void MappingRDFToESServer() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TopDoubleObjectCollection<String> searchKeyWords(Client client, String index, String type, String value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getIndexName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isESRunning() {
		// TODO Auto-generated method stub
		return false;
	}
}
