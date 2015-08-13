package org.aksw.simba.tapioca.extraction.voidex;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.aksw.simba.tapioca.extraction.AbstractExtractor;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class VoidExtractor extends AbstractExtractor {

	private static final Set<Node> CLASS_RESOURCES = new HashSet<Node>(Arrays.asList(RDFS.Class.asNode(),
			OWL.Class.asNode(), OWL.DeprecatedClass.asNode()));
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
		Node predicate = triple.getPredicate();
		Node object = triple.getObject();
		if (predicate.equals(RDF.type.asNode()) && !(object.isBlank())) {
			if (CLASS_RESOURCES.contains(object)) {
				countedClasses.putOrAdd(triple.getSubject().getURI(), 0, 0);
			} else if (PROPERTY_RESOURCES.contains(object)) {
				countedProperties.putOrAdd(triple.getSubject().getURI(), 0, 0);
			}
			countedClasses.putOrAdd(object.getURI(), 1, 1);
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
