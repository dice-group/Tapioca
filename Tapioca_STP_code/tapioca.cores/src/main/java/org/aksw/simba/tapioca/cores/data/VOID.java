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
package org.aksw.simba.tapioca.cores.data;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

/**
 * <p>
 * VoID is an RDF Schema vocabulary for expressing metadata about RDF datasets.
 * It is intended as a bridge between the publishers and users of RDF data, with
 * applications ranging from data discovery to cataloging and archiving of
 * datasets. This document is a detailed guide to the VoID vocabulary. It
 * describes how VoID can be used to express general metadata based on Dublin
 * Core, access metadata, structural metadata, and links between datasets. It
 * also provides deployment advice and discusses the discovery of VoID
 * descriptions.
 * </p>
 * 
 * The VOID reference can be found at http://www.w3.org/TR/void/
 */
public class VOID {

	protected static final String uri = "http://rdfs.org/ns/void#";

	/**
	 * returns the URI for this schema
	 * 
	 * @return the URI for this schema
	 */
	public static String getURI() {
		return uri;
	}

	protected static final Resource resource(String local) {
		return ResourceFactory.createResource(uri + local);
	}

	protected static final Property property(String local) {
		return ResourceFactory.createProperty(uri, local);
	}

	/*
	 * Classes
	 */

	/**
	 * A set of RDF triples that are published, maintained or aggregated by a
	 * single provider.
	 */
	public static final Resource Dataset = resource("Dataset");
	/**
	 * A web resource whose foaf:primaryTopic or foaf:topics include
	 * void:Datasets.
	 */
	public static final Resource DatasetDescription = resource("DatasetDescription");
	/**
	 * A collection of RDF links between two void:Datasets.
	 */
	public static final Resource Linkset = resource("Dataset");
	/**
	 * A technical feature of a void:Dataset, such as a supported RDF
	 * serialization format.
	 */
	public static final Resource TechnicalFeature = resource("Dataset");

	/*
	 * Properties
	 */

	/**
	 * The rdfs:Class that is the rdf:type of all entities in a class-based
	 * partition.
	 */
	public static final Property clazz = property("class");
	/**
	 * The total number of distinct classes in a void:Dataset.
	 */
	public static final Property classes = property("classes");
	/**
	 * A subset of a void:Dataset that contains only the entities of a certain
	 * rdfs:Class.
	 */
	public static final Property classPartition = property("classPartition");
	/**
	 * An RDF dump, partial or complete, of a void:Dataset.
	 */
	public static final Property dataDump = property("dataDump");
	/**
	 * The total number of distinct objects in a void:Dataset.
	 */
	public static final Property distinctObjects = property("distinctObjects");
	/**
	 * The total number of distinct subjects in a void:Dataset.
	 */
	public static final Property distinctSubjects = property("distinctSubjects");
	/**
	 * The total number of documents, for void:Datasets that are published as a
	 * set of individual RDF documents.
	 */
	public static final Property documents = property("documents");
	/**
	 * The total number of entities that are described in a void:Dataset.
	 */
	public static final Property entities = property("entities");
	/**
	 * An example entity that is representative for the entities described in a
	 * void:Dataset.
	 */
	public static final Property exampleResource = property("exampleResource");
	/**
	 * A void:TechnicalFeature supported by a void:Datset.
	 */
	public static final Property feature = property("feature");
	/**
	 * Points to the void:Dataset that a document is a part of.
	 */
	public static final Property inDataset = property("inDataset");
	/**
	 * Specifies the RDF property of the triples in a void:Linkset.
	 */
	public static final Property linkPredicate = property("linkPredicate");
	/**
	 * The void:Dataset that contains the resources in the object position of a
	 * void:Linkset's triples.
	 */
	public static final Property objectsTarget = property("objectsTarget");
	/**
	 * An OpenSearch description document for a free-text search service over a
	 * void:Dataset.
	 */
	public static final Property openSearchDescription = property("openSearchDescription");
	/**
	 * The total number of distinct properties in a void:Dataset.
	 */
	public static final Property properties = property("properties");
	/**
	 * The rdf:Property that is the predicate of all triples in a property-based
	 * partition.
	 */
	public static final Property property = property("property");
	/**
	 * A subset of a void:Dataset that contains only the triples of a certain
	 * rdf:Property.
	 */
	public static final Property propertyPartition = property("propertyPartition");
	/**
	 * A top concept or entry point for a void:Dataset that is structured in a
	 * tree-like fashion.
	 */
	public static final Property rootResource = property("rootResource");
	/**
	 * A SPARQL protocol endpoint that allows SPARQL query access to a
	 * void:Dataset.
	 */
	public static final Property sparqlEndpoint = property("sparqlEndpoint");
	/**
	 * The void:Dataset that contains the resources in the subject position of
	 * this void:Linkset's triples.
	 */
	public static final Property subjectsTarget = property("subjectsTarget");
	/**
	 * A void:Dataset that is part of another void:Dataset.
	 */
	public static final Property subset = property("subset");
	/**
	 * One of the two void:Datasets connected by this void:Linkset.
	 */
	public static final Property target = property("target");
	/**
	 * The total number of triples contained in a void:Dataset.
	 */
	public static final Property triples = property("triples");
	/**
	 * A protocol endpoint for simple URI lookups for a void:Dataset.
	 */
	public static final Property uriLookupEndpoint = property("uriLookupEndpoint");
	/**
	 * A regular expression that matches the URIs of a void:Dataset's entities.
	 */
	public static final Property uriRegexPattern = property("uriRegexPattern");
	/**
	 * A URI that is a common string prefix of all the entity URIs in a
	 * void:Datset.
	 */
	public static final Property uriSpace = property("uriSpace");
	/**
	 * A vocabulary or owl:Ontology whose classes or properties are used in a
	 * void:Dataset.
	 */
	public static final Property vocabulary = property("vocabulary");
}
