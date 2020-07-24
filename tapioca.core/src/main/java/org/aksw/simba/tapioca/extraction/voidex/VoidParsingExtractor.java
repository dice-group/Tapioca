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
/**
 * This file is part of tapioca.core.
 *
 * tapioca.core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.core.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.extraction.voidex;

import java.util.HashSet;
import java.util.Set;

import org.aksw.simba.tapioca.data.vocabularies.VOID;
import org.aksw.simba.tapioca.extraction.AbstractExtractor;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;

/**
 * A simple extractor that searches for VOID class and property partitions and
 * read their counts (if these counts are available).
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class VoidParsingExtractor extends AbstractExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(VoidParsingExtractor.class);

    /**
     * Static rdf:type URI for faster access.
     */
    private static final String RDF_TYPE_URI = RDF.type.getURI();
    /**
     * Static void:Dataset URI for faster access.
     */
    private static final String VOID_DATASET_URI = VOID.Dataset.getURI();

    /**
     * List of class and property informations gathered so far.
     */
    private ObjectObjectOpenHashMap<String, VoidInformation> voidInformation;
    /**
     * List of blacklisted URIs (mainly dataset URIs that could be handled as class
     * or property information by mistake).
     */
    private Set<String> blacklist = new HashSet<>();

    public VoidParsingExtractor() {
        this.voidInformation = new ObjectObjectOpenHashMap<String, VoidInformation>();
    }

    public VoidParsingExtractor(ObjectObjectOpenHashMap<String, VoidInformation> voidInformation) {
        this.voidInformation = voidInformation;
    }

    public void handleTriple(Triple triple) {
        Node subject = triple.getSubject();
        Node predicate = triple.getPredicate();
        Node object = triple.getObject();
        if (predicate.getURI().startsWith(VOID.getURI())) {
            // This predicate is part of the VOID vocabulary
            String subjectURI = subject.isBlank() ? subject.getBlankNodeLabel() : subject.getURI();
            if (!blacklist.contains(subjectURI)) {
                if (predicate.equals(VOID.clazz.asNode())) {
                    // We found a class
                    ClassDescription classDesc;
                    if (voidInformation.containsKey(subjectURI)) {
                        classDesc = (ClassDescription) voidInformation.get(subjectURI);
                    } else {
                        classDesc = new ClassDescription();
                        voidInformation.put(subjectURI, classDesc);
                    }
                    classDesc.uri = object.getURI();
                } else if (predicate.equals(VOID.entities.asNode()) && object.isLiteral()) {
                    // We found an entity count that most likely belongs to a class
                    ClassDescription classDesc = null;
                    if (voidInformation.containsKey(subjectURI)) {
                        // First, check whether the found information is a class description
                        VoidInformation info = voidInformation.get(subjectURI);
                        if (info instanceof ClassDescription) {
                            classDesc = (ClassDescription) info;
                        } else {
                            // This is not a class description but most probably a dataset. Remove it from
                            // the map and add it to the blacklist
                            voidInformation.remove(subjectURI);
                            blacklist.add(subjectURI);
                        }
                    } else {
                        classDesc = new ClassDescription();
                        voidInformation.put(subjectURI, classDesc);
                    }
                    if (classDesc != null) {
                        classDesc.count = parseInteger(object);
                    }
                } else if (predicate.equals(VOID.property.asNode())) {
                    // We found a property
                    PropertyDescription propertyDesc;
                    if (voidInformation.containsKey(subjectURI)) {
                        propertyDesc = (PropertyDescription) voidInformation.get(subjectURI);
                    } else {
                        propertyDesc = new PropertyDescription();
                        voidInformation.put(subjectURI, propertyDesc);
                    }
                    propertyDesc.uri = object.getURI();
                } else if (predicate.equals(VOID.triples.asNode())) {
                    // We found an entity count that most likely belongs to a property
                    PropertyDescription propertyDesc = null;
                    if (voidInformation.containsKey(subjectURI)) {
                        // First, check whether the found information is a property description
                        VoidInformation info = voidInformation.get(subjectURI);
                        if (info instanceof PropertyDescription) {
                            propertyDesc = (PropertyDescription) info;
                        } else {
                            // This is not a property description but most probably a dataset. Remove it
                            // from the map and add it to the blacklist
                            voidInformation.remove(subjectURI);
                            blacklist.add(subjectURI);
                        }
                    } else {
                        propertyDesc = new PropertyDescription();
                        voidInformation.put(subjectURI, propertyDesc);
                    }
                    if (propertyDesc != null) {
                        propertyDesc.count = parseInteger(object);
                    }
                }
            }
        } else if (RDF_TYPE_URI.equals(predicate.getURI()) && object.isURI()
                && VOID_DATASET_URI.equals(object.getURI())) {
            blacklist.add(subject.isBlank() ? subject.getBlankNodeLabel() : subject.getURI());
        }
    }

    protected int parseInteger(Node object) {
        Object value = object.getLiteralValue();
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Long) {
            return ((Long) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        } else {
            LOGGER.error("Got an unknown literal type \"" + value.getClass().toString() + "\".");
        }
        return 0;
    }

    public ObjectObjectOpenHashMap<String, VoidInformation> getVoidInformation() {
        return voidInformation;
    }

    public void setVoidInformation(ObjectObjectOpenHashMap<String, VoidInformation> voidInformation) {
        this.voidInformation = voidInformation;
    }

}
