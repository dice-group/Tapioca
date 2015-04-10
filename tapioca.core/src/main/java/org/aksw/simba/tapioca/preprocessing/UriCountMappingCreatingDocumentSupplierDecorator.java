package org.aksw.simba.tapioca.preprocessing;

import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetLODStatsInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.data.DatasetSpecialClassesInfo;
import org.aksw.simba.tapioca.data.StringCountMapping;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyAppendingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UriCountMappingCreatingDocumentSupplierDecorator extends
        AbstractPropertyAppendingDocumentSupplierDecorator<StringCountMapping> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(UriCountMappingCreatingDocumentSupplierDecorator.class);

    public static enum UriUsage {
        CLASSES,
        PROPERTIES,
        CLASSES_AND_PROPERTIES,
        EXTENDED_CLASSES,
        EXTENDED_CLASSES_AND_PROPERTIES
    }

    private UriUsage usage;

    public UriCountMappingCreatingDocumentSupplierDecorator(DocumentSupplier documentSource, UriUsage usage) {
        super(documentSource);
        this.usage = usage;
    }

    @Override
    protected StringCountMapping createPropertyForDocument(Document document) {
        StringCountMapping mapping = new StringCountMapping();
        switch (usage) {
        case EXTENDED_CLASSES_AND_PROPERTIES: {
            addSpecialClasses(document, mapping);
            // falls through
        }
        case CLASSES_AND_PROPERTIES: {
            addClasses(document, mapping);
            // falls through
        }
        case PROPERTIES: {
            addProperties(document, mapping);
            break;
        }
        case EXTENDED_CLASSES: {
            addSpecialClasses(document, mapping);
            // falls through
        }
        case CLASSES: {
            addClasses(document, mapping);
            break;
        }
        }
        return mapping;
    }

    private void addClasses(Document document, StringCountMapping mapping) {
        DatasetLODStatsInfo infoProperty = document.getProperty(DatasetClassInfo.class);
        if (infoProperty == null) {
            LOGGER.error("Got a document without the needed DatasetLODStatsClassInfo property. Can't add any class URIs.");
        } else {
            add(infoProperty, mapping);
        }
    }

    private void addSpecialClasses(Document document, StringCountMapping mapping) {
        DatasetLODStatsInfo infoProperty = document.getProperty(DatasetSpecialClassesInfo.class);
        if (infoProperty == null) {
            LOGGER.error("Got a document without the needed DatasetLODStatsSpecialClassesInfo property. Can't add any class URIs.");
        } else {
            add(infoProperty, mapping);
        }
    }

    private void addProperties(Document document, StringCountMapping mapping) {
        DatasetLODStatsInfo infoProperty = document.getProperty(DatasetPropertyInfo.class);
        if (infoProperty == null) {
            LOGGER.error("Got a document without the needed DatasetLODStatsPropertyInfo property. Can't add any property URIs.");
        } else {
            add(infoProperty, mapping);
        }
    }

    private void add(DatasetLODStatsInfo infoProperty, StringCountMapping mapping) {
        mapping.get().putAll(infoProperty.get());
    }

}
