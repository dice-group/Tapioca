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

import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyEditingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.AbstractSimpleDocumentProperty;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import com.carrotsearch.hppc.ObjectOpenHashSet;

public class SimpleBlankNodeRemovingDocumentSupplierDecorator<T extends AbstractSimpleDocumentProperty<ObjectLongOpenHashMap<String>>>
        extends AbstractPropertyEditingDocumentSupplierDecorator<T> {

    public SimpleBlankNodeRemovingDocumentSupplierDecorator(DocumentSupplier documentSource, Class<T> propertyClass) {
        super(documentSource, propertyClass);
    }

    @Override
    protected void editDocumentProperty(T property) {
        ObjectLongOpenHashMap<String> map = property.get();
        ObjectOpenHashSet<String> removableURIs = new ObjectOpenHashSet<String>();
        String uri;
        for (int i = 0; i < map.allocated.length; ++i) {
            if (map.allocated[i]) {
                uri = (String) ((Object[]) map.keys)[i];
                // If this URI seems to be a blank node
                if (uri.startsWith("_:node") || uri.startsWith("node")) {
                    removableURIs.add(uri);
                }
            }
        }
        map.removeAll(removableURIs);
    }

}
