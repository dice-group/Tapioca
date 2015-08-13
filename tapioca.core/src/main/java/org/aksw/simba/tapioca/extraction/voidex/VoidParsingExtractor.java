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
			if (predicate.equals(VOID.clazz.asNode())) {
				ClassDescription classDesc;
				if (voidInformation.containsKey(subjectURI)) {
					classDesc = (ClassDescription) voidInformation.get(subjectURI);
				} else {
					classDesc = new ClassDescription();
					voidInformation.put(subjectURI, classDesc);
				}
				classDesc.uri = object.getURI();
			} else if (predicate.equals(VOID.entities.asNode()) && object.isLiteral()) {
				ClassDescription classDesc;
				if (voidInformation.containsKey(subjectURI)) {
					classDesc = (ClassDescription) voidInformation.get(subjectURI);
				} else {
					classDesc = new ClassDescription();
					voidInformation.put(subjectURI, classDesc);
				}
				classDesc.count = parseInteger(object);
			} else if (predicate.equals(VOID.property.asNode())) {
				PropertyDescription propertyDesc;
				if (voidInformation.containsKey(subjectURI)) {
					propertyDesc = (PropertyDescription) voidInformation.get(subjectURI);
				} else {
					propertyDesc = new PropertyDescription();
					voidInformation.put(subjectURI, propertyDesc);
				}
				propertyDesc.uri = object.getURI();
			} else if (predicate.equals(VOID.triples.asNode())) {
				PropertyDescription propertyDesc;
				if (voidInformation.containsKey(subjectURI)) {
					propertyDesc = (PropertyDescription) voidInformation.get(subjectURI);
				} else {
					propertyDesc = new PropertyDescription();
					voidInformation.put(subjectURI, propertyDesc);
				}
				propertyDesc.count = parseInteger(object);
			}
		}
	}

	protected int parseInteger(Node object) {
		Object value = object.getLiteralValue();
		if (value instanceof Integer) {
			return (Integer) value;
		} else if (value instanceof Long) {
			return ((Long) value).intValue();
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
