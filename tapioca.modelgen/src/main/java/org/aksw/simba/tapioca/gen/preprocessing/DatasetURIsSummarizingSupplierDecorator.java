package org.aksw.simba.tapioca.gen.preprocessing;

import org.aksw.simba.tapioca.data.StringCountMapping;
import org.aksw.simba.tapioca.gen.data.DatasetURIs;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyAppendingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectOpenHashSet;

public class DatasetURIsSummarizingSupplierDecorator extends
        AbstractPropertyAppendingDocumentSupplierDecorator<DatasetURIs> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetURIsSummarizingSupplierDecorator.class);

    public DatasetURIsSummarizingSupplierDecorator(DocumentSupplier documentSource) {
        super(documentSource);
    }

    @Override
    protected DatasetURIs createPropertyForDocument(Document document) {
        StringCountMapping countedURIs = document.getProperty(StringCountMapping.class);
        if (countedURIs == null) {
            LOGGER.error("Got a document without the needed StringCountMapping property. Ignoring it.");
            return null;
        } else {
            return new DatasetURIs(new ObjectOpenHashSet<String>(countedURIs.get().keys()));
        }
    }

}
