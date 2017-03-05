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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.aksw.simba.topicmodeling.algorithms.ProbabilisticWordTopicModel;
import org.aksw.simba.topicmodeling.commons.collections.TopDoubleObjectCollection;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.vocabulary.RDFS;
/**
 * @author Duong T.D.
 */
public class ESEngine extends AbstractEngine {
	/**
	 * Logging
	 */
    private static final Logger LOGGER = LoggerFactory.getLogger(ESEngine.class);

    /**
     * Number of results
     */
    private static final int DEFAULT_NUMBER_OF_RESULTS = 20;
    
    /**
     * Number of results
     */
    private int numberOfResults = DEFAULT_NUMBER_OF_RESULTS;    
    
    /**
     * Index Name
     */
    private static final String INDEX_NAME = "tapioca";

    /**
     * Document Type
     */
    private static final String DOCUMENT_TYPE = "METADATA";
    
    /**
     * TransportClient
     */    
    private TransportClient transportClient;
            
    /**
     * Constructor
     * @param transportClient
     * @param rdfMetaDataModel
     */    
	protected ESEngine(TransportClient transportClient, Model rdfMetaDataModel) {
		super(transportClient, rdfMetaDataModel);
		this.transportClient = transportClient;		
		LOGGER.info("ESEngine created.");		
	}

    /**
     * Call the constructor
     * @param transportClient 
     * @param rdfMetaDataModelPath 
     * @return ESEngine
     */	
	public static ESEngine createEngine(TransportClient transportClient, Model rdfMetaDataModel){
        return new ESEngine(transportClient, rdfMetaDataModel);    	
	}

    /**
     * Mapping the properties of RDF meta model to Elasticsearch Server
     * @param  
     * @return 
     */	
	public void MappingRDFToESServer() throws IOException, InterruptedException{		
        String indexName = INDEX_NAME;
        String documentType = DOCUMENT_TYPE;
        
        //check if index is exist for a new clean index
        IndicesExistsResponse resource = transportClient.admin().indices().prepareExists(indexName).execute().actionGet();        
        if (resource.isExists()) {
            DeleteIndexRequestBuilder deleteIndex = transportClient.admin().indices().prepareDelete(indexName);
            deleteIndex.execute().actionGet();
            LOGGER.info("Index already exists, creating new clean index...");
        }
        CreateIndexRequestBuilder createIndexRequestBuilder = transportClient.admin().indices().prepareCreate(indexName);
        
		// construct mapping
        XContentBuilder builder = jsonBuilder()
        		.startObject()
        			.startObject(documentType)
        				.startObject("_meta")
	        				.field("Title", "string")
	        				.field("URI", "uri")
	        				.field("Description", "string")
        				.endObject()
        			.endObject()
                .endObject();        
        createIndexRequestBuilder.addMapping(documentType, builder);
        createIndexRequestBuilder.execute().actionGet();
        		                     
        ResIterator listResources = rdfMetaDataModel.listSubjects();
        long documentID = 0;
        while (listResources.hasNext())
        {
    		String uri = listResources.next().toString();
    		String titel = rdfMetaDataModel.listStatements(new ResourceImpl(uri), RDFS.label, (RDFNode) null)
    				.next().getObject().toString();
    		String description = rdfMetaDataModel.listStatements(new ResourceImpl(uri), RDFS.comment, (RDFNode) null)
    				.next().getObject().toString();
    		
	        documentID++;
	        // Add documents
	        IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex(indexName, documentType, "" + documentID);
	        // build json object
	        final XContentBuilder contentBuilder = jsonBuilder().startObject().prettyPrint();
    		
	        contentBuilder.field("Title", titel)
  			  			  .field("URI", uri)
  			  			  .field("Description", description);
	        indexRequestBuilder.setSource(contentBuilder);
	        indexRequestBuilder.execute().actionGet();
        }
		LOGGER.info( "RDF metadatamodel has been successfully mapped to elasticserach server.");        
	}
	
	/**
	 * Search with keyword
	 * 
	 * @param client
	 * @param type
	 * @param field
	 * @param keywords 
	 * @return
	 */	
	public TopDoubleObjectCollection<String> searchKeyWords(Client client, String index, String type, String keywords) {
        LOGGER.info("Searching keyword...");                
        //build query
		SearchResponse response = client.prepareSearch(index)
										.setTypes(type)
										.setSearchType(SearchType.QUERY_AND_FETCH)
										.setQuery(QueryBuilders.boolQuery().should(QueryBuilders.queryString(keywords)))
										.setFrom(0)
										.setSize(numberOfResults)
										.setExplain(true)										
										.execute()
										.actionGet();
		SearchHit[] resultSearchHit = response.getHits().getHits();
		Map<String, Float> resultMap = new LinkedHashMap<String, Float>();
        
		//only take the result with highest score				
		for (SearchHit hit : resultSearchHit) {
			String key = hit.getSource().get("URI").toString();
			Float value = resultMap.get(key);									
			if (value == null || value < hit.getScore()/10){
				resultMap.put(key, hit.getScore()/10);
			}
		}		
		
		//not necessary anymore, but for safety... 
		int count = 0;				
		TopDoubleObjectCollection<String> mostSimilarDatasets = 
				new TopDoubleObjectCollection<String>((resultMap.size()<numberOfResults ? resultMap.size() : numberOfResults), true);		
        for (String key : resultMap.keySet()) {
			mostSimilarDatasets.add(resultMap.get(key), key);
			if (++count >= numberOfResults) break;
        }
        LOGGER.info("Done.");				
		return mostSimilarDatasets;
	}    
	
    /**
     * Check if elasticsearch server is running
     * @param client
     * @return
     */	
    public boolean isESRunning() {
        ImmutableList<DiscoveryNode> nodes = transportClient.connectedNodes();
        if (nodes.isEmpty()) {
        	LOGGER.error("No nodes available. The elasticsearch server my not be running.");
        	return false;
        } else 
            LOGGER.info("Connected to nodes: " + nodes.toString());
        return true;
    }	
    
    /**
     * Get index name
     * @return INDEX_NAME
     */    	
	public String getIndexName(){
		return INDEX_NAME;
	}
	
    /**
     * Get document type
     * @return DOCUMENT_TYPE
     */    	
	public String getType() {
		return DOCUMENT_TYPE;
	}
		
    /**
     * Get transport client
     * @return transportClient
     */    	
	public TransportClient getTransportClient() {
		return transportClient;
	}

	@Override
	public Corpus getCorpus() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public ProbabilisticWordTopicModel getModel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TopDoubleObjectCollection<String> retrieveSimilarDatasets(Document document) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Document preprocess(String voidString) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getWorkProgress() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setWorkProgress(Integer value) {
		// TODO Auto-generated method stub
		
	}
}
