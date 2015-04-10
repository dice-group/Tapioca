package org.aksw.simba.tapioca.preprocessing;

import java.util.ArrayList;
import java.util.List;

import org.aksw.simba.tapioca.data.SimpleTokenizedText;
import org.aksw.simba.tapioca.data.StringCountMapping;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyAppendingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;

public class StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator extends
        AbstractPropertyAppendingDocumentSupplierDecorator<SimpleTokenizedText> {

    public static enum WordOccurence {
        UNIQUE,
        LOG
    }

    private static final Logger LOGGER = LoggerFactory
            .getLogger(StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.class);

    private WordOccurence occurence;

    public StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator(DocumentSupplier documentSource,
            WordOccurence occurence) {
        super(documentSource);
        this.occurence = occurence;
    }

    @Override
    protected SimpleTokenizedText createPropertyForDocument(Document document) {
        StringCountMapping mapping = document.getProperty(StringCountMapping.class);
        if (mapping == null) {
            LOGGER.error("Got a document without the needed StringCountMapping property. Ignoring this document.");
        } else {
            return createSimpleTokenizedText(mapping);
        }
        return null;
    }

    private SimpleTokenizedText createSimpleTokenizedText(StringCountMapping mapping) {
        ObjectLongOpenHashMap<String> counts = mapping.get();
        String token;
        long count;
        List<String> tokens = new ArrayList<String>(counts.size());
        for (int i = 0; i < counts.allocated.length; ++i) {
            if (counts.allocated[i]) {
                token = (String) ((Object[]) counts.keys)[i];
                count = counts.values[i];

                switch (occurence) {
                case UNIQUE: {
                    count = 1;
                    break;
                }
                case LOG: {
                    count = count > 1 ? Math.round(Math.log(count)) + 1 : 1;
                    break;
                }
                }

                for (int j = 0; j < count; ++j) {
                    tokens.add(token);
                }
            }
        }
        return new SimpleTokenizedText(tokens.toArray(new String[tokens.size()]));
    }
}
