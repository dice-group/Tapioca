/**
 * tapioca.analyzer - ${project.description}
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
 * This file is part of tapioca.analyzer.
 *
 * tapioca.analyzer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.analyzer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.analyzer.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.analyzer.label;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.data.vocabularies.VOID;
import org.aksw.simba.tapioca.preprocessing.JenaBasedVoidParsingSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.LabelTokenizerHelper;
import org.aksw.simba.topicmodeling.io.SimpleDocSupplierFromFile;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentTextCreatingSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;

public class LabelExtractionUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(LabelExtractionUtils.class);

    public static Set<String> readUris(File voidFile) {
        SimpleDocSupplierFromFile reader = new SimpleDocSupplierFromFile();
        reader.createRawDocumentAdHoc(voidFile);
        DocumentSupplier supplier = new DocumentTextCreatingSupplierDecorator(reader);
        supplier = new JenaBasedVoidParsingSupplierDecorator(supplier);

        Document document = supplier.getNextDocument();
        DatasetClassInfo classInfo = document.getProperty(DatasetClassInfo.class);
        Set<String> uris = new HashSet<String>();
        ObjectLongOpenHashMap<String> countedURIs;
        if (classInfo != null) {
            countedURIs = classInfo.get();
            for (int i = 0; i < countedURIs.allocated.length; ++i) {
                if (countedURIs.allocated[i]) {
                    uris.add((String) ((Object[]) countedURIs.keys)[i]);
                }
            }
        } else {
            LOGGER.error("Document doesn't contain class information. Returning null.");
            return null;
        }

        DatasetPropertyInfo propInfo = document.getProperty(DatasetPropertyInfo.class);
        if (propInfo != null) {
            countedURIs = propInfo.get();
            for (int i = 0; i < countedURIs.allocated.length; ++i) {
                if (countedURIs.allocated[i]) {
                    uris.add((String) ((Object[]) countedURIs.keys)[i]);
                }
            }
        } else {
            LOGGER.error("Document doesn't contain property information. Returning null.");
            return null;
        }
        return uris;
    }
    
    public static Set<String> readUris(Model voidModel) {
        Set<String> uris = new HashSet<String>();
        Property properties[] = new Property[] {VOID.clazz, VOID.property};
        NodeIterator iterator;
        RDFNode n;
        for (int i = 0; i < properties.length; i++) {
            iterator = voidModel.listObjectsOfProperty(properties[i]);
            while(iterator.hasNext()) {
                n = iterator.next();
                if(n.isURIResource()) {
                    uris.add(n.asResource().getURI());
                }
            }
        }
        return uris;
    }

    /**
     * Generates an array containing the information of the given labels map.
     * <p>
     * The method creates an array with the size <code>labels.size()</code>
     * containing a String array for every URI. This array starts with the URI
     * itself at position <code>0</code>. The other positions (&gt;=1) contain
     * the single tokens of the labels of this URI.
     * </p>
     * 
     * @param labels
     *            a mapping of URIs and a Set containing their labels
     * @return the generated String array
     */
    public static String[][] generateArray(Map<String, Set<String>> labels) {
        String uriToLabel[][] = new String[labels.size()][];
        int pos = 0;
        String tokenizedLabel[];
        for (Entry<String, Set<String>> labelsOfUri : labels.entrySet()) {
            tokenizedLabel = tokenize(labelsOfUri.getValue());
            if (tokenizedLabel != null) {
                uriToLabel[pos] = new String[tokenizedLabel.length + 1];
                System.arraycopy(tokenizedLabel, 0, uriToLabel[pos], 1, tokenizedLabel.length);
            } else {
                uriToLabel[pos] = new String[1];
            }
            uriToLabel[pos][0] = labelsOfUri.getKey();
            ++pos;
        }
        return uriToLabel;
    }

    public static String[] tokenize(Set<String> names) {
        HashSet<String> uniqueLabels = new HashSet<String>();
        for (String label : names) {
            uniqueLabels.addAll(LabelTokenizerHelper.getSeparatedText(label));
        }
        return uniqueLabels.toArray(new String[uniqueLabels.size()]);
    }
}
