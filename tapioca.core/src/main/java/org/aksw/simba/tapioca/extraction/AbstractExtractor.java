package org.aksw.simba.tapioca.extraction;

import org.apache.jena.riot.lang.PipedRDFIterator;

import com.hp.hpl.jena.graph.Triple;

public abstract class AbstractExtractor implements Extractor {

	public void extract(PipedRDFIterator<Triple> iter) {
		while (iter.hasNext()) {
			handleTriple(iter.next());
		}
	}
}
