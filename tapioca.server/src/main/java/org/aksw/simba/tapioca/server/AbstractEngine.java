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

import java.io.StringWriter;

import org.dice_research.topicmodeling.commons.collections.TopDoubleObjectCollection;
import org.dice_research.topicmodeling.preprocessing.SingleDocumentPreprocessor;
import org.dice_research.topicmodeling.utils.doc.Document;
import org.dice_research.topicmodeling.utils.doc.DocumentText;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;

public abstract class AbstractEngine implements Engine {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEngine.class);

    protected SingleDocumentPreprocessor preprocessor;
    protected Model rdfMetaDataModel;

    protected AbstractEngine(SingleDocumentPreprocessor preprocessor, Model rdfMetaDataModel) {
        this.preprocessor = preprocessor;
        this.rdfMetaDataModel = rdfMetaDataModel;
    }

    @Override
    public String retrieveSimilarDatasets(String voidString) {
        // preprocess given void string
        LOGGER.info("Preprocessing void string...");
        Document document = preprocess(voidString);
        if (document == null) {
            throw new IllegalArgumentException("The given void string did not result in a valid document.");
        }
        TopDoubleObjectCollection<String> mostSimilarDatasets = retrieveSimilarDatasets(document);
        return generateResultString(mostSimilarDatasets);
    }

    protected abstract TopDoubleObjectCollection<String> retrieveSimilarDatasets(Document document);

    protected synchronized Document preprocess(String voidString) {
        Document document = new Document();
        document.addProperty(new DocumentText(voidString));
        return preprocessor.processDocument(document);
    }

    protected String generateResultString(TopDoubleObjectCollection<String> mostSimilarDatasets) {
        Model rdfModel = ModelFactory.createDefaultModel();
        Property simProperty = rdfModel.createProperty(TAPIOCA_SIMILARITY_URI);
        Resource datasetResource;
        for (int i = 0; i < mostSimilarDatasets.values.length; ++i) {
            datasetResource = rdfModel.createResource((String) mostSimilarDatasets.objects[i]);
            rdfModel.addLiteral(datasetResource, simProperty, mostSimilarDatasets.values[i]);
            if (rdfMetaDataModel.containsResource(datasetResource)) {
                rdfModel.add(rdfMetaDataModel.listStatements(datasetResource, null, (RDFNode) null));
                // /// TODO REMOVE ME!!!
                // if (i < 7) {
                // StmtIterator iter;
                // System.out.println();
                // System.out.println(datasetResource.getURI());
                // iter = rdfMetaDataModel.listStatements(datasetResource,
                // rdfMetaDataModel.createProperty("http://www.w3.org/2000/01/rdf-schema#label"),
                // (RDFNode) null);
                // System.out.print("label: ");
                // if (iter.hasNext()) {
                // System.out.print(iter.next().getObject().asLiteral().getString());
                // }
                // System.out.println();
                // iter = rdfMetaDataModel.listStatements(datasetResource,
                // rdfMetaDataModel.createProperty("http://www.w3.org/2000/01/rdf-schema#comment"),
                // (RDFNode) null);
                // System.out.print("description: ");
                // if (iter.hasNext()) {
                // System.out.print(iter.next().getObject().asLiteral().getString());
                // }
                // System.out.println();
                // iter = rdfMetaDataModel.listStatements(datasetResource,
                // rdfMetaDataModel.createProperty("http://www.w3.org/ns/dcat#keyword"),
                // (RDFNode) null);
                // System.out.print("keywords: ");
                // boolean first = true;
                // while (iter.hasNext()) {
                // if(first) {
                // first = false;
                // } else {
                // System.out.print(", ");
                // }
                // System.out.print('"');
                // System.out.print(iter.next().getObject().asLiteral().getString());
                // System.out.print('"');
                // }
                // System.out.println();
                // }
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
