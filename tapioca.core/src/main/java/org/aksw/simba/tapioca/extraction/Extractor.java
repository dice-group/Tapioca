package org.aksw.simba.tapioca.extraction;

import org.apache.jena.riot.lang.PipedRDFIterator;

import com.hp.hpl.jena.graph.Triple;

public interface Extractor {

	public void extract(PipedRDFIterator<Triple> iter);

	public void handleTriple(Triple triple);
}
