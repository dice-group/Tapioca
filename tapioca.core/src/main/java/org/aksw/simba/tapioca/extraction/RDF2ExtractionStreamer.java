package org.aksw.simba.tapioca.extraction;

import java.io.InputStream;
import java.io.StringReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;

import com.hp.hpl.jena.graph.Triple;

/**
 * Class to manage the streaming of RDF from a source (a String or an
 * InputStream) to a set of {@link Extractor} instances. Internally it is based
 * on at least one further thread that is used for the streaming itself (as
 * described in the Jena RDF streaming description). During the streaming the
 * {@link Extractor#handleTriple(Triple)} is called.
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class RDF2ExtractionStreamer {

	private ExecutorService executor;

	public RDF2ExtractionStreamer() {
		this.executor = Executors.newSingleThreadExecutor();
	}

	public RDF2ExtractionStreamer(ExecutorService executor) {
		this.executor = executor;
	}

	public void runExtraction(String rdfData, String baseUri, Lang language, Extractor... extractors) {
		PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
		PipedRDFStream<Triple> rdfStream = new PipedTriplesStream(iter);
		String2RDFStreamingTask parserTask = new String2RDFStreamingTask(rdfStream, new StringReader(rdfData), baseUri,
				language);
		executor.execute(parserTask);
	}

	public void runExtraction(InputStream is, String baseUri, Lang language, Extractor... extractors) {
		PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
		PipedRDFStream<Triple> rdfStream = new PipedTriplesStream(iter);
		runExtraction(iter, new InputStream2RDFStreamingTask(rdfStream, is, baseUri, language), extractors);
	}

	protected void runExtraction(PipedRDFIterator<Triple> iter, Runnable task, Extractor extractors[]) {
		executor.execute(task);
		runExtraction(iter, extractors);
	}

	protected void runExtraction(PipedRDFIterator<Triple> iter, Extractor extractors[]) {
		Triple triple;
		while (iter.hasNext()) {
			triple = iter.next();
			for (int i = 0; i < extractors.length; ++i) {
				extractors[i].handleTriple(triple);
			}
		}
	}
}
