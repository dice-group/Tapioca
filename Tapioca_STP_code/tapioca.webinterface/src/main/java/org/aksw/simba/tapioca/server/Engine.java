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

import java.io.IOException;
import org.aksw.simba.topicmodeling.algorithms.ProbabilisticWordTopicModel;
import org.aksw.simba.topicmodeling.commons.collections.TopDoubleObjectCollection;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;

import com.hp.hpl.jena.rdf.model.Model;

public interface Engine {
    public static final String TAPIOCA_SIMILARITY_URI = "http://tapioca.aksw.org/vocabulary/similarity";    
    //Engine    
	public Integer getWorkProgress();		
	public void setWorkProgress(Integer value);	
    public Model getRDFMetaModel();    
    //TMEngine Methods
    public Document preprocess(String voidString);
    public Corpus getCorpus();
    public ProbabilisticWordTopicModel getModel();  
    public TopDoubleObjectCollection<String> retrieveSimilarDatasets(Document document);
    //ESEngine Methods    
    public TransportClient getTransportClient();        
    public void MappingRDFToESServer() throws IOException, InterruptedException;
    public TopDoubleObjectCollection<String> searchKeyWords(Client client, String index, String type, String value);
    public boolean isESRunning();
    public String getIndexName();
    public String getType();
}
