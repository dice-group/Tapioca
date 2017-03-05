/**
 * This file is part of tapioca.cores.
 *
 * tapioca.cores is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.cores is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.cores.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.cores.extraction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.aksw.simba.tapioca.cores.extraction.AbstractExtractor;
import org.aksw.simba.tapioca.cores.helper.Extractor;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * Count classes and properties within a data set
 * 
 * @author Michael Roeder, Kai
 */
public class VoidExtractor extends AbstractExtractor implements Extractor {
	/**
	 * Define classes
	 */
    private static final Set<Node> CLASS_RESOURCES = new HashSet<Node>(
            Arrays.asList(RDFS.Class.asNode(), OWL.Class.asNode(), OWL.DeprecatedClass.asNode()));
    
    /**
     * Define properties
     */
    private static final Set<Node> PROPERTY_RESOURCES = new HashSet<Node>(Arrays.asList(RDF.Property.asNode(),
            OWL.AnnotationProperty.asNode(), OWL.DatatypeProperty.asNode(), OWL.DeprecatedProperty.asNode(),
            OWL.FunctionalProperty.asNode(), OWL.InverseFunctionalProperty.asNode(), OWL.ObjectProperty.asNode(),
            OWL.OntologyProperty.asNode(), OWL.SymmetricProperty.asNode(), OWL.TransitiveProperty.asNode()));

    /**
     * Store counted classes
     */
    private ObjectIntOpenHashMap<String> countedClasses;
    
    /**
     * Store counted properties
     */
    private ObjectIntOpenHashMap<String> countedProperties;

    /**
     * Constructor
     */
    public VoidExtractor() {
        this.countedClasses = new ObjectIntOpenHashMap<String>();
        this.countedProperties = new ObjectIntOpenHashMap<String>();
    }

    /**
     * Constructor
     * @param countedClasses already counted classes
     * @param countedProperties already counted properties
     */
    public VoidExtractor(ObjectIntOpenHashMap<String> countedClasses, ObjectIntOpenHashMap<String> countedProperties) {
        this.countedClasses = countedClasses;
        this.countedProperties = countedProperties;
    }
    
    /**
     * Apply this to every triple in the stream
     */
    public void handleTriple(Triple triple) {
        Node subject = triple.getSubject();
        Node predicate = triple.getPredicate();
        Node object = triple.getObject();
        if (predicate.equals(RDF.type.asNode()) && !(object.isBlank())) {
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

    /**
     * Get countedClasses
     * @return countedClasses The counted classes
     */
    public ObjectIntOpenHashMap<String> getCountedClasses() {
        return countedClasses;
    }

    /**
     * Set countedClasses
     * @param countedClasses The countedClasses
     */
    public void setCountedClasses(ObjectIntOpenHashMap<String> countedClasses) {
        this.countedClasses = countedClasses;
    }

    /**
     * Get countedProperties
     * @return countedProperties The counted properties
     */
    public ObjectIntOpenHashMap<String> getCountedProperties() {
        return countedProperties;
    }

    /**
     * Set countedProperties
     * @param countedProperties The counted properties
     */
    public void setCountedProperties(ObjectIntOpenHashMap<String> countedProperties) {
        this.countedProperties = countedProperties;
    }

}
