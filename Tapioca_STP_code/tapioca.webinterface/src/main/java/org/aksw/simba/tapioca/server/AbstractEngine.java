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
import java.io.StringWriter;

import org.aksw.simba.topicmodeling.commons.collections.TopDoubleObjectCollection;
import org.aksw.simba.topicmodeling.preprocessing.SingleDocumentPreprocessor;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.n3.turtle.TurtleReader;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.RDFReader;
import com.hp.hpl.jena.rdf.model.Resource;

public abstract class AbstractEngine implements Engine {

	/**
	 * Logging
	 */
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEngine.class);

    /**
     * Preprocessor
     */
    protected SingleDocumentPreprocessor preprocessor;

    /**
     * TransportClient
     */
    protected TransportClient transportClient;
    
    /**
     * RDF Metadata Model
     */
    protected Model rdfMetaDataModel;

    /**
     * Constructor
     * @param preprocessor Preprocessor
     * @param rdfMetaDataModel RDF Metadata Model
     */
    protected AbstractEngine(SingleDocumentPreprocessor preprocessor, Model rdfMetaDataModel) {
        this.preprocessor = preprocessor;
        this.rdfMetaDataModel = rdfMetaDataModel;
    }

    /**
     * Constructor
     * @param transportClient TransportClient
     * @param rdfMetaDataModel RDF Metadata Model
     */
    protected AbstractEngine(TransportClient transportClient, Model rdfMetaDataModel) {
    	this.transportClient = transportClient;
        this.rdfMetaDataModel = rdfMetaDataModel;
    }    

    /**
     * Get the meta model
     * @return meta model
     */        
    public Model getRDFMetaModel(){
    	return rdfMetaDataModel;
    }
    
    public static Model readRDFMetaDataFile(File rdfMetaDataFile){
        RDFReader reader = new TurtleReader();
        Model rdfMetaDataModel = ModelFactory.createDefaultModel();
        
        LOGGER.info("Loading meta data file from " + rdfMetaDataFile.getAbsolutePath());
        FileInputStream fin = null;
        try {
            fin = new FileInputStream(rdfMetaDataFile);
            reader.read(rdfMetaDataModel, fin, "");
        } catch (FileNotFoundException e) {
            LOGGER.error("Couldn't read meta data from file. Returning null.", e);
            return null;
        } finally {
            IOUtils.closeQuietly(fin);
        }  
        return rdfMetaDataModel;
    }
    
            
    /**
     * Generate result string
     * @param mostSimilarDatasets datasets as collection
     * @return String
     */
    protected String generateResultString(TopDoubleObjectCollection<String> mostSimilarDatasets) {
        Model rdfModel = ModelFactory.createDefaultModel();
        Property simProperty = rdfModel.createProperty(TAPIOCA_SIMILARITY_URI);
        Resource datasetResource;
        for (int i = 0; i < mostSimilarDatasets.values.length; ++i) {
            datasetResource = rdfModel.createResource((String) mostSimilarDatasets.objects[i]);
            rdfModel.addLiteral(datasetResource, simProperty, mostSimilarDatasets.values[i]);
            if (rdfMetaDataModel.containsResource(datasetResource)) {
                rdfModel.add(rdfMetaDataModel.listStatements(datasetResource, null, (RDFNode) null));
            } else {
                LOGGER.warn("Got a dataset that does not occur inside the meta data model (URI=\""
                        + datasetResource.getURI() + "\").");
            }
        }

        StringWriter stringWriter = new StringWriter();
        RDFDataMgr.write(stringWriter, rdfModel, RDFFormat.JSONLD);
        String result = stringWriter.toString();
        IOUtils.closeQuietly(stringWriter);
        return result;
    }
}
