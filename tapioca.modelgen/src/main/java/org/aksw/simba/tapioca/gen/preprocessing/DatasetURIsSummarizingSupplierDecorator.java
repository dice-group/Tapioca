/**
 * tapioca.modelgen - ${project.description}
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
 * This file is part of tapioca.modelgen.
 *
 * tapioca.modelgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.modelgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.modelgen.  If not, see <http://www.gnu.org/licenses/>.
 */
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
