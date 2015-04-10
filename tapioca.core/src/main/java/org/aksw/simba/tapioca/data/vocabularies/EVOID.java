package org.aksw.simba.tapioca.data.vocabularies;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class EVOID {

    protected static final String uri = "http://aksw.org/ns/extended-void#";

    /**
     * returns the URI for this schema
     * 
     * @return the URI for this schema
     */
    public static String getURI() {
        return uri;
    }

    protected static final Property property(String local) {
        return ResourceFactory.createProperty(uri, local);
    }

    public static final Property specialClass = property("specialClass");
    public static final Property classPartition = property("classPartition");
    public static final Property entities = property("entities");

}
