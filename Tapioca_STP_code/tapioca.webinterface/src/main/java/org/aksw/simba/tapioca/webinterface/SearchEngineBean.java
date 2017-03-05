/**
 * This class represents the "model" part in the MVC 
 * pattern
 */
package org.aksw.simba.tapioca.webinterface;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.faces.bean.ApplicationScoped;
import javax.faces.bean.ManagedBean;

import org.aksw.simba.tapioca.cores.data.Dataset;
import org.aksw.simba.tapioca.cores.data.VOID;
import org.aksw.simba.tapioca.cores.preprocessing.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.server.AbstractEngine;
import org.aksw.simba.tapioca.server.ESEngine;
import org.aksw.simba.tapioca.server.Engine;
import org.aksw.simba.tapioca.server.TMEngine;
import org.aksw.simba.tapioca.server.data.SearchResult;
import org.aksw.simba.topicmodeling.commons.collections.TopDoubleObjectCollection;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentName;
import org.aksw.simba.topicmodeling.utils.doc.DocumentURI;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.vocabulary.RDFS;

@ManagedBean(name = "searchEngineBean", eager=true )
@ApplicationScoped
public class SearchEngineBean {
	
	/**
	 * Logging
	 */
	static {System.setProperty("logback.configurationFile",  
			new File(SearchEngineBean.class.getProtectionDomain().getCodeSource().getLocation().getFile()).getAbsolutePath().substring
			(0, new File(SearchEngineBean.class.getProtectionDomain().getCodeSource().getLocation().getFile()).getAbsolutePath().
			lastIndexOf("tapioca.webinterface/WEB-INF/")) + "tapioca.webinterface/WEB-INF/classes//logback.xml");}		
	private static final Logger LOGGER = LoggerFactory.getLogger(SearchEngineBean.class);
	
	/**
	 * The Topic Model Engine
	 */
	private static Engine topicmodelEngine;	

	/**
	 * The Elasticsearch Engine
	 */
	private static Engine elasticsearchEngine;	
	
    /**
     * Label cache files
     */
	private String[] cachefiles;
    
    /**
     * Model file
     */
	private String modelfile;

	/**
	 * Corpus file
	 */
	private String corpusfile;
	
	/**
	 * RDF meta data file
	 */
	private String rdfMetaDataFile;

	/**
	 * RDF meta data model
	 */
	private Model rdfMetaDataModel;
	
	/**
	 * Elasticsearch port number
	 */
	private int elasticport;

	/**
	 * Input document
	 */
	private Document inputDocument;	
	
	/**
     * Constructor
     */
	public SearchEngineBean() {
		try {
			//initialize
			Configuration conf = new Configuration();
			cachefiles = conf.getCachefiles();
			modelfile = conf.getModelfile();
			corpusfile = conf.getCorpusfile();
			rdfMetaDataFile = conf.getMetadatafile();
			elasticport = conf.getElasticport();
			// create
			createSearchEngineBean();
		} catch( Exception e ) {
			LOGGER.error( "Failed to create SearchEngineBean.", e );
		}
	}
	
	/**
	 * Constructor
	 * @param cachefiles
	 * @param modelfile
	 * @param corpusfile
	 * @param rdfMetaDataFile
	 */
	protected SearchEngineBean( String[] cachefiles,
			String modelfile, String corpusfile, String rdfMetaDataFile, int elasticport ) {
		try {
			// initialize
			this.cachefiles = cachefiles;
			this.modelfile = modelfile;
			this.corpusfile = corpusfile;
			this.rdfMetaDataFile = rdfMetaDataFile;
			this.elasticport = elasticport;
			// create
			createSearchEngineBean();
		} catch( Exception e ) {
			LOGGER.error( "Failed to create SearchEngineBean.", e );
		}
	}
	
	private void createSearchEngineBean() throws Exception {
		// read RDF meta data
		rdfMetaDataModel = AbstractEngine.readRDFMetaDataFile(new File( rdfMetaDataFile ) );
		
		// create decorator
		WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever = createCachingLabelRetriever();
		
		// create transportClient
		TransportClient transportClient = createTransportClient( elasticport );
		
		//create engine
		topicmodelEngine = createEngine(cachingLabelRetriever);
		elasticsearchEngine = createEngine(transportClient);
		
		// start mapping
		if( elasticsearchEngine.isESRunning() ) {
			LOGGER.info( "Mapping RDF meta model to elasticsearch server..." );
			elasticsearchEngine.MappingRDFToESServer();
		}
		else {
			LOGGER.warn( "Failed to create mapping. Search with keywords can not be used." );
		}
		
		// report success
		LOGGER.info( "SearchEngineBean created." );
	}
	    
    /**
     * Create TMEngine
     * @param cachingLabelRetriever
     * @return
     */
    private TMEngine createEngine(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {
        return TMEngine.createEngine(cachingLabelRetriever, new File(modelfile),
                new File(corpusfile), rdfMetaDataModel);
    }

    /**
     * Create ESEngine
     * @param cachingLabelRetriever
     * @return
     */
    private ESEngine createEngine(TransportClient transportClient) {
        return ESEngine.createEngine(transportClient, rdfMetaDataModel);
    }
    
	/**
	 * Create Caching label retriever
	 * @return WorkerBasedLabelRetrievingDocumentSupplierDecorator
	 */
    private WorkerBasedLabelRetrievingDocumentSupplierDecorator createCachingLabelRetriever() {
        File labelFiles[] = {}; // TODO add label files
        String cacheFileNames[] = cachefiles;
        File chacheFiles[] = new File[cacheFileNames.length];
        for (int i = 0; i < chacheFiles.length; ++i) {
            chacheFiles[i] = new File(cacheFileNames[i]);
        }
        return new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, chacheFiles, labelFiles);
    }

	/**
	 * Create transport client
	 * @param port
	 * @return TransportClient
	 */    
    @SuppressWarnings("resource")
	private TransportClient createTransportClient(int port){
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
        TransportClient client = new TransportClient(settings);
        try{
        	client = client.addTransportAddress(new InetSocketTransportAddress("localhost", port));
	    } catch(Exception e) {
	    	LOGGER.error("Couldn't create transport client on Port: " + port + ". ", e);
	    }        
        return client;
    }
            
	/**
	 * Create document
	 * @param inputModel Input model
	 * @return Document
	 */
	protected Document createDocument(Model inputModel) {				
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		RDFDataMgr.write(os, inputModel, Lang.NTRIPLES);
		int numberOfRetrievalLabels = (getNumberOfRetrievalLabels(inputModel)==0) ? 1 
									  : getNumberOfRetrievalLabels(inputModel);
		WorkerBasedLabelRetrievingDocumentSupplierDecorator.setCurrentProgress(0);
		
        Thread thread = new Thread(new Runnable() {
            public void run() {
				inputDocument = topicmodelEngine.preprocess(new String(os.toByteArray()));
            }
        });
        thread.start();
        while (thread.isAlive()) {    
    		topicmodelEngine.setWorkProgress(
    				(int)(100*((double)WorkerBasedLabelRetrievingDocumentSupplierDecorator.getWorkProgress()
    						 /(double)numberOfRetrievalLabels)));
    		//update process every 0.5s
        	try {
        	    Thread.sleep(500);                
        	} catch(InterruptedException ex) {
        	    Thread.currentThread().interrupt();
        	}       	       	
        }	
        return inputDocument;
	}
	
	/**
	 * Count retrieval labels  
	 * @param inputModel Input model
	 * @return integer
	 */	
	public int getNumberOfRetrievalLabels(Model inputModel){
		int count = 0;
        StmtIterator iter = inputModel.listStatements();
        while (iter.hasNext()){
            Triple triple = iter.nextStatement().asTriple();
            Node predicate = triple.getPredicate();
            Node object = triple.getObject();

            if (object.isURI() && predicate.isURI() && 
            		(predicate.getURI().equals(VOID.clazz.getURI()) ||
            				predicate.getURI().equals(VOID.property.getURI()))) count++;
        }				
        return count;
	}

	/**
	 * Execute search
	 * @param inputText
	 * @return
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public List<SearchResult> run(String inputText) throws IOException, InterruptedException {
		TopDoubleObjectCollection<String> mostSimilarDatasets = elasticsearchEngine
				.searchKeyWords(elasticsearchEngine.getTransportClient(),
								elasticsearchEngine.getIndexName(), 
								elasticsearchEngine.getType(),
								inputText);
		
		// initialize
		List<SearchResult> searchResults  = new ArrayList<SearchResult>();
		Model metaDataModel = topicmodelEngine.getRDFMetaModel();
		
		// set result
        for (int i=mostSimilarDatasets.values.length - 1; i>=0; i--) {
			Statement s;
			String uri = (String)mostSimilarDatasets.objects[i];
			String titel = "";
			String description = "";
			double similary = mostSimilarDatasets.values[i];
			if (metaDataModel.containsResource(new ResourceImpl(uri))) {
				s = metaDataModel.listStatements(new ResourceImpl(uri), RDFS.label, (RDFNode) null).next();
				titel += s.getObject().toString();
				s = metaDataModel.listStatements(new ResourceImpl(uri), RDFS.comment, (RDFNode) null).next();
				description += s.getObject().toString();
			}

			if (titel.equals("")) {
				titel = (String) mostSimilarDatasets.objects[i];
			}

			if (description.equals("")) {
				description = "No description.";
			}

			// create datasets
			searchResults.add(new SearchResult(new Dataset(titel, uri, description), similary));
		}

		// transmit to bean
		return searchResults;
	}
	
		
	/**
	 * Execute search
	 * @param inputModel
	 * @return
	 */
	public List<SearchResult> run(Model inputModel) {	                
		TopDoubleObjectCollection<String> mostSimilarDatasets = topicmodelEngine.retrieveSimilarDatasets(inputDocument);
		
		// initialize
		List<SearchResult> searchResults  = new ArrayList<SearchResult>();
		Corpus corpus = topicmodelEngine.getCorpus();
		Model metaDataModel = topicmodelEngine.getRDFMetaModel();
		
		// set result
		mostSimilarDatasets = toAscendingOrder(mostSimilarDatasets);
        for (int i=mostSimilarDatasets.values.length - 1; i>=0; i--) {
        	Statement s;
    		String uri = "";
    		String titel = "";    		
    		String description = "";
    		double similary = mostSimilarDatasets.values[i];
        	for (int j=0;j<corpus.getNumberOfDocuments();j++){
        		Document document = corpus.getDocument(j);
        		if (document.getProperty(DocumentURI.class).getStringValue().equals((String)mostSimilarDatasets.objects[i])){
        			uri = document.getProperty( DocumentURI.class ).getStringValue();
        			
        			if (metaDataModel.containsResource(new ResourceImpl(uri))) {
        				s = metaDataModel.listStatements(new ResourceImpl(uri), RDFS.label, (RDFNode) null).next();
        				titel += s.getObject().toString();
        				s = metaDataModel.listStatements(new ResourceImpl(uri), RDFS.comment, (RDFNode) null).next();
        				description += s.getObject().toString();
        			}
        			
        			if (titel.equals("")){
        				titel = "(ID: " + document.getDocumentId() + ") " + document.getProperty(DocumentName.class).getName();
        			}

        			if (description.equals("")){
        				description = "No description.";
        			}        			
        			break;
        		}
        	}
        	
    		// create datasets    		
			searchResults.add(new SearchResult(new Dataset(titel, uri, description ), similary));
        }
		// transmit to bean
		return searchResults;
	}	
	
	/**
	 * Get topic model engine
	 */
	public static Engine getTMEngine(){
		return topicmodelEngine;
	}	
	
	/**
	 * Get elasticsearch engine
	 */
	public static Engine getESEngine(){
		return elasticsearchEngine;
	}
		
	protected TopDoubleObjectCollection<String> toAscendingOrder(TopDoubleObjectCollection<String> topDoubleObjectCollection){
        TopDoubleObjectCollection<String> results = new TopDoubleObjectCollection<String>(topDoubleObjectCollection.values.length, true);		
        for (int i = 0; i < topDoubleObjectCollection.values.length; ++i) {
        	results.add(topDoubleObjectCollection.values[i], (String)topDoubleObjectCollection.objects[i]);
        }		
		return results;
	}		
}
