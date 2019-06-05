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

import java.util.Set;

import org.dice_research.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyEditingDocumentSupplierDecorator;
import org.dice_research.topicmodeling.utils.doc.AbstractSimpleDocumentProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import com.carrotsearch.hppc.ObjectOpenHashSet;

public class UriFilteringDocumentSupplierDecorator<T extends AbstractSimpleDocumentProperty<ObjectLongOpenHashMap<String>>>
        extends AbstractPropertyEditingDocumentSupplierDecorator<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UriFilteringDocumentSupplierDecorator.class);

    private String blacklist[];

    public UriFilteringDocumentSupplierDecorator(DocumentSupplier documentSource, Set<String> blacklist,
            Class<T> propertyClass) {
        super(documentSource, propertyClass);
        // Set<String> blacklist = VoidExtractor.loadVocabBlacklist();
        if (blacklist == null) {
            LOGGER.error("Couldn't load blacklist. This filter will be unable to work as expected.");
            this.blacklist = new String[0];
        } else {
            this.blacklist = blacklist.toArray(new String[blacklist.size()]);
        }
    }

    @Override
    protected void editDocumentProperty(T mapping) {
        ObjectLongOpenHashMap<String> map = mapping.get();
        ObjectOpenHashSet<String> removableURIs = new ObjectOpenHashSet<String>();
        String uri;
        int pos;
        for (int i = 0; i < map.allocated.length; ++i) {
            if (map.allocated[i]) {
                uri = (String) ((Object[]) map.keys)[i];
                pos = 0;
                while ((pos < blacklist.length) && (!uri.startsWith(blacklist[pos]))) {
                    ++pos;
                }
                // If this URI's vocabulary is on the blacklist
                if (pos < blacklist.length) {
                    removableURIs.add(uri);
                }
            }
        }
        map.removeAll(removableURIs);
    }
}
