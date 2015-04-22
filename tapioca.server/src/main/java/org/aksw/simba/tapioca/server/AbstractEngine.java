package org.aksw.simba.tapioca.server;

import java.io.StringWriter;

import org.aksw.simba.topicmodeling.commons.collections.TopDoubleObjectCollection;
import org.aksw.simba.topicmodeling.preprocessing.SingleDocumentPreprocessor;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentText;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

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
            rdfModel.createLiteralStatement(datasetResource, simProperty, mostSimilarDatasets.values[i]);
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
