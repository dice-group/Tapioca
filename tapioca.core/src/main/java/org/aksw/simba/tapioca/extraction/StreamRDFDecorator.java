package org.aksw.simba.tapioca.extraction;

import org.apache.jena.riot.system.StreamRDF;

public interface StreamRDFDecorator extends StreamRDF {

    public StreamRDF getDecorated();
}
