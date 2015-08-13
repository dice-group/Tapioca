package org.aksw.simba.tapioca.extraction;

import java.io.StringReader;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFStream;

import com.hp.hpl.jena.graph.Triple;

public class String2RDFStreamingTask implements Runnable {

	private PipedRDFStream<Triple> rdfStream;
	private StringReader reader;
	private String baseUri;
	private Lang language;

	public String2RDFStreamingTask(PipedRDFStream<Triple> rdfStream, StringReader reader, String baseUri, Lang language) {
		this.rdfStream = rdfStream;
		this.reader = reader;
		this.baseUri = baseUri;
		this.language = language;
	}

	@Override
	public void run() {
		// Call the parsing process.
		RDFDataMgr.parse(rdfStream, reader, baseUri, language);
	}
}
