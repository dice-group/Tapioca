/**
 * tapioca.core - ${project.description}
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
