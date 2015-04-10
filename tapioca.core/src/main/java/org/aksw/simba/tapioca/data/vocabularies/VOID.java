package org.aksw.simba.tapioca.data.vocabularies;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

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

    public static final Resource dataset = resource("dataset");

    public static final Property clazz = property("class");
    public static final Property classes = property("classes");
    public static final Property classPartition = property("classPartition");
    public static final Property distinctSubjects = property("distinctSubjects");
    public static final Property distinctObjects = property("distinctObjects");
    public static final Property entities = property("entities");
    public static final Property properties = property("properties");
    public static final Property property = property("property");
    public static final Property propertyPartition = property("propertyPartition");
    public static final Property subset = property("subset");
    public static final Property triples = property("triples");
    public static final Property vocabulary = property("vocabulary");

}
