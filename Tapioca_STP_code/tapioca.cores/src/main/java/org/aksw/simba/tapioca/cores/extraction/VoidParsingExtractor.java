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

import org.aksw.simba.tapioca.cores.data.ClassDescription;
import org.aksw.simba.tapioca.cores.data.PropertyDescription;
import org.aksw.simba.tapioca.cores.data.VOID;
import org.aksw.simba.tapioca.cores.data.VoidInformation;
import org.aksw.simba.tapioca.cores.extraction.AbstractExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

/**
 * Search for already present VoID information
 * @author Michael Roeder, Kai
 * 
 */
public class VoidParsingExtractor extends AbstractExtractor implements Extractor, org.aksw.simba.tapioca.cores.helper.Extractor {
	
	/**
	 * Logging
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(VoidParsingExtractor.class);

	/**
	 * Store VoID information
	 */
	private ObjectObjectOpenHashMap<String, VoidInformation> voidInformation;

	/**
	 * Constructor
	 */
	public VoidParsingExtractor() {
		this.voidInformation = new ObjectObjectOpenHashMap<String, VoidInformation>();
	}

	/**
	 * Constructor
	 * @param voidInformation already present VoID information
	 */
	public VoidParsingExtractor(ObjectObjectOpenHashMap<String, VoidInformation> voidInformation) {
		this.voidInformation = voidInformation;
	}

	/**
	 * Apply this to every triple in the stream
	 */
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
					// new cast
					classDesc = voidInformation.get(subjectURI).toClass();
				} else {
					classDesc = new ClassDescription();
					voidInformation.put(subjectURI, classDesc);
				}
				classDesc.uri = object.getURI();
			} else if (predicate.equals(VOID.entities.asNode()) && object.isLiteral()) {
				ClassDescription classDesc;
				if (voidInformation.containsKey(subjectURI)) {
					// new cast
					classDesc = voidInformation.get(subjectURI).toClass();
				} else {
					classDesc = new ClassDescription();
					voidInformation.put(subjectURI, classDesc);
				}
				classDesc.count = parseInteger(object);
			} else if (predicate.equals(VOID.property.asNode())) {
				PropertyDescription propertyDesc;
				if (voidInformation.containsKey(subjectURI)) {
					// new cast
					propertyDesc = voidInformation.get(subjectURI).toProperty();
				} else {
					propertyDesc = new PropertyDescription();
					voidInformation.put(subjectURI, propertyDesc);
				}
				propertyDesc.uri = object.getURI();
			} else if (predicate.equals(VOID.triples.asNode())) {
				PropertyDescription propertyDesc;
				if (voidInformation.containsKey(subjectURI)) {
					// new cast
					propertyDesc = voidInformation.get(subjectURI).toProperty();
				} else {
					propertyDesc = new PropertyDescription();
					voidInformation.put(subjectURI, propertyDesc);
				}
				propertyDesc.count = parseInteger(object);
			}
		}
	}

	/**
	 * Transform into INT value 
	 * @param object node
	 * @return The INT value
	 */
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

	/**
	 * Get voidInformation
	 * @return voidInformation 
	 */
	public ObjectObjectOpenHashMap<String, VoidInformation> getVoidInformation() {
		return voidInformation;
	}

	/**
	 * Set voidInformation
	 * @param voidInformation voidInformation 
	 */
	public void setVoidInformation(ObjectObjectOpenHashMap<String, VoidInformation> voidInformation) {
		this.voidInformation = voidInformation;
	}

}
