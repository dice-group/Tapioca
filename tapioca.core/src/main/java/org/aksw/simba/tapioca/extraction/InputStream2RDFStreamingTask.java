package org.aksw.simba.tapioca.extraction;

import java.io.InputStream;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;

public class InputStream2RDFStreamingTask implements Runnable {

    private StreamRDF rdfStream;
	private InputStream is;
	private String baseUri;
	private Lang language;

	public InputStream2RDFStreamingTask(StreamRDF rdfStream, InputStream is, String baseUri, Lang language) {
		this.rdfStream = rdfStream;
		this.is = is;
		this.baseUri = baseUri;
	}

	@Override
	public void run() {
		// Call the parsing process.
		RDFDataMgr.parse(rdfStream, is, baseUri, language);
        System.out.println("Finished streaming...");
	}
}
