package org.aksw.simba.tapioca.extraction.voidex;

import org.aksw.simba.tapioca.data.vocabularies.VOID;
import org.aksw.simba.tapioca.extraction.AbstractExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

public class VoidParsingExtractor extends AbstractExtractor {

	private static final Logger LOGGER = LoggerFactory.getLogger(VoidParsingExtractor.class);

	private ObjectObjectOpenHashMap<String, VoidInformation> voidInformation;

	public VoidParsingExtractor(ObjectObjectOpenHashMap<String, VoidInformation> voidInformation) {
		this.voidInformation = voidInformation;
	}

	public void handleTriple(Triple triple) {
		Node subject = triple.getSubject();
		Node predicate = triple.getPredicate();
		Node object = triple.getObject();
		if (predicate.getURI().startsWith(VOID.getURI())) {
			// This predicate is part of the VOID vocabulary
			String subjectURI = subject.getURI();
			if (predicate.equals(VOID.clazz)) {
				ClassDescription classDesc;
				if (voidInformation.containsKey(subjectURI)) {
					classDesc = (ClassDescription) voidInformation.get(subjectURI);
				} else {
					classDesc = new ClassDescription();
					voidInformation.put(subjectURI, classDesc);
				}
				classDesc.uri = object.getURI();
			} else if (predicate.equals(VOID.entities)) {
				ClassDescription classDesc;
				if (voidInformation.containsKey(subjectURI)) {
					classDesc = (ClassDescription) voidInformation.get(subjectURI);
				} else {
					classDesc = new ClassDescription();
					voidInformation.put(subjectURI, classDesc);
				}
				try {
					classDesc.count = Integer.parseInt(object.toString());
				} catch (Exception e) {
					LOGGER.error("Tried to parse the entities count from \"" + triple.toString() + "\".", e);
				}
			} else if (predicate.equals(VOID.property)) {
				PropertyDescription propertyDesc;
				if (voidInformation.containsKey(subjectURI)) {
					propertyDesc = (PropertyDescription) voidInformation.get(subjectURI);
				} else {
					propertyDesc = new PropertyDescription();
					voidInformation.put(subjectURI, propertyDesc);
				}
				propertyDesc.uri = object.getURI();
			} else if (predicate.equals(VOID.triples)) {
				PropertyDescription propertyDesc;
				if (voidInformation.containsKey(subjectURI)) {
					propertyDesc = (PropertyDescription) voidInformation.get(subjectURI);
				} else {
					propertyDesc = new PropertyDescription();
					voidInformation.put(subjectURI, propertyDesc);
				}
				try {
					propertyDesc.count = Integer.parseInt(object.toString());
				} catch (Exception e) {
					LOGGER.error("Tried to parse the entities count from \"" + triple.toString() + "\".", e);
				}
			}
		}
	}

	public ObjectObjectOpenHashMap<String, VoidInformation> getVoidInformation() {
		return voidInformation;
	}

	public void setVoidInformation(ObjectObjectOpenHashMap<String, VoidInformation> voidInformation) {
		this.voidInformation = voidInformation;
	}

}
