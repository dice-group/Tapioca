package org.aksw.simba.tapioca.extraction.voidex;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.aksw.simba.tapioca.extraction.AbstractExtractor;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class VoidExtractor extends AbstractExtractor {

	private static final Set<Resource> CLASS_RESOURCES = new HashSet<Resource>(Arrays.asList(RDFS.Class, OWL.Class,
			OWL.DeprecatedClass));
	private static final Set<Resource> PROPERTY_RESOURCES = new HashSet<Resource>(Arrays.asList(RDF.Property,
			OWL.AnnotationProperty, OWL.DatatypeProperty, OWL.DeprecatedProperty, OWL.FunctionalProperty,
			OWL.InverseFunctionalProperty, OWL.ObjectProperty, OWL.OntologyProperty, OWL.SymmetricProperty,
			OWL.TransitiveProperty));

	private ObjectIntOpenHashMap<String> countedClasses;
	private ObjectIntOpenHashMap<String> countedProperties;

	public VoidExtractor(ObjectIntOpenHashMap<String> countedClasses, ObjectIntOpenHashMap<String> countedProperties) {
		this.countedClasses = countedClasses;
		this.countedProperties = countedProperties;
	}

	public void handleTriple(Triple triple) {
		Node subject = triple.getSubject();
		Node predicate = triple.getPredicate();
		Node object = triple.getObject();
		if (predicate.equals(RDF.type) && !(subject.isBlank())) {
			if (CLASS_RESOURCES.contains(object)) {
				countedClasses.putOrAdd(subject.getURI(), 0, 0);
			} else if (PROPERTY_RESOURCES.contains(object)) {
				countedProperties.putOrAdd(subject.getURI(), 0, 0);
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
