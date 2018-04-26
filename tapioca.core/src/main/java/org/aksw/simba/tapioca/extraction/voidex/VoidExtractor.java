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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.aksw.simba.tapioca.extraction.AbstractExtractor;
import org.aksw.simba.tapioca.extraction.Extractor;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

/**
 * <p>
 * This {@link Extractor} takes triples and generates VoID metadata from them.
 * Therefore the class counts properties and triples based on certain rules. The
 * following rules are programmed into this class.
 * </p>
 * 
 * <table>
 * <tr>
 * <th>subject</th>
 * <th>predicate</th>
 * <th>object</th>
 * <th>action</th>
 * </tr>
 * <tr>
 * <td>*</td>
 * <td>*</td>
 * <td>*</td>
 * <td>The predicate URI is added to the property counting Map and its count is
 * increased by 1.</td>
 * </tr>
 * <tr>
 * <td>*</td>
 * <td>rdf:type</td>
 * <td>not blank</td>
 * <td>The object URI is added to the class counting Map and its count is
 * increased by 1.</td>
 * </tr>
 * <tr>
 * <td>not blank</td>
 * <td>rdf:type</td>
 * <td>is in <b><i>C</i></b></td>
 * <td>The subject URI is added to the class counting Map (without increasing
 * the count).</td>
 * </tr>
 * <tr>
 * <td>not blank</td>
 * <td>rdf:type</td>
 * <td>is in <b><i>P</i></b></td>
 * <td>The subject URI is added to the property counting Map (without increasing
 * the count).</td>
 * </tr>
 * <tr>
 * <td>not blank</td>
 * <td>rdf:subClassOf</td>
 * <td>*</td>
 * <td>The subject URI is added to the class counting Map (without increasing
 * the count).</td>
 * </tr>
 * <tr>
 * <td>*</td>
 * <td>rdf:subClassOf</td>
 * <td>not blank</td>
 * <td>The object URI is added to the class counting Map (without increasing the
 * count).</td>
 * </tr>
 * <tr>
 * <td>not blank</td>
 * <td>rdf:subPropertyOf</td>
 * <td>*</td>
 * <td>The subject URI is added to the property counting Map (without increasing
 * the count).</td>
 * </tr>
 * <tr>
 * <td>*</td>
 * <td>rdf:subPropertyOf</td>
 * <td>not blank</td>
 * <td>The object URI is added to the property counting Map (without increasing
 * the count).</td>
 * </tr>
 * </table>
 * 
 * <p>
 * The set <b><i>C</i></b> contains
 * <ul>
 * <li>rdfs:Class</li>
 * <li>owl:Class</li>
 * <li>owl:DeprecatedClass</li>
 * </ul>
 * </p>
 * 
 * <p>
 * The set <b><i>P</i></b> contains
 * <ul>
 * <li>rdf:Property</li>
 * <li>owl:AnnotationProperty</li>
 * <li>owl:DatatypeProperty</li>
 * <li>owl:DeprecatedProperty</li>
 * <li>owl:FunctionalProperty</li>
 * <li>owl:InverseFunctionalProperty</li>
 * <li>owl:ObjectProperty</li>
 * <li>owl:OntologyProperty</li>
 * <li>owl:SymmetricProperty</li>
 * <li>owl:TransitiveProperty</li>
 * </ul>
 * </p>
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class VoidExtractor extends AbstractExtractor {

    private static final Set<Node> CLASS_RESOURCES = new HashSet<Node>(
            Arrays.asList(RDFS.Class.asNode(), OWL.Class.asNode(), OWL.DeprecatedClass.asNode()));
    private static final Set<Node> PROPERTY_RESOURCES = new HashSet<Node>(Arrays.asList(RDF.Property.asNode(),
            OWL.AnnotationProperty.asNode(), OWL.DatatypeProperty.asNode(), OWL.DeprecatedProperty.asNode(),
            OWL.FunctionalProperty.asNode(), OWL.InverseFunctionalProperty.asNode(), OWL.ObjectProperty.asNode(),
            OWL.OntologyProperty.asNode(), OWL.SymmetricProperty.asNode(), OWL.TransitiveProperty.asNode()));

    private ObjectIntOpenHashMap<String> countedClasses;
    private ObjectIntOpenHashMap<String> countedProperties;

    public VoidExtractor() {
        this.countedClasses = new ObjectIntOpenHashMap<String>();
        this.countedProperties = new ObjectIntOpenHashMap<String>();
    }

    public VoidExtractor(ObjectIntOpenHashMap<String> countedClasses, ObjectIntOpenHashMap<String> countedProperties) {
        this.countedClasses = countedClasses;
        this.countedProperties = countedProperties;
    }

    public void handleTriple(Triple triple) {
        Node subject = triple.getSubject();
        Node predicate = triple.getPredicate();
        Node object = triple.getObject();
        if (predicate.equals(RDF.type.asNode()) && !(object.isBlank())) {
            // TODO add test case in which the subject is a blank node but is
            // defined as a class or a property
            if (!subject.isBlank()) {
                if (CLASS_RESOURCES.contains(object)) {
                    countedClasses.putOrAdd(subject.getURI(), 0, 0);
                } else if (PROPERTY_RESOURCES.contains(object)) {
                    countedProperties.putOrAdd(subject.getURI(), 0, 0);
                }
            }
            countedClasses.putOrAdd(object.getURI(), 1, 1);
        } else if (predicate.equals(RDFS.subClassOf.asNode())) {
            // TODO add a test case for this
            if (!subject.isBlank()) {
                countedClasses.putOrAdd(subject.getURI(), 0, 0);
            }
            if (!object.isBlank()) {
                countedClasses.putOrAdd(object.getURI(), 0, 0);
            }
        } else if (predicate.equals(RDFS.subPropertyOf.asNode())) {
            // TODO add a test case for this
            if (!subject.isBlank()) {
                countedProperties.putOrAdd(subject.getURI(), 0, 0);
            }
            if (!object.isBlank()) {
                countedProperties.putOrAdd(object.getURI(), 0, 0);
            }
        }
        // count the property
        countedProperties.putOrAdd(predicate.getURI(), 1, 1);
    }

    public ObjectIntOpenHashMap<String> getCountedClasses() {
        return countedClasses;
    }

    public void setCountedClasses(ObjectIntOpenHashMap<String> countedClasses) {
        this.countedClasses = countedClasses;
    }

    public ObjectIntOpenHashMap<String> getCountedProperties() {
        return countedProperties;
    }

    public void setCountedProperties(ObjectIntOpenHashMap<String> countedProperties) {
        this.countedProperties = countedProperties;
    }

}
