package org.aksw.simba.tapioca.extraction;

import java.io.StringReader;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;

public class String2RDFStreamingTask implements Runnable {

    private StreamRDF rdfStream;
    private StringReader reader;
    private String baseUri;
    private Lang language;

    public String2RDFStreamingTask(StreamRDF rdfStream, StringReader reader, String baseUri,
            Lang language) {
        this.rdfStream = rdfStream;
        this.reader = reader;
        this.baseUri = baseUri;
        this.language = language;
    }

    @Override
    public void run() {
        // Call the parsing process.
        RDFDataMgr.parse(rdfStream, reader, baseUri, language);
        System.out.println("Finished streaming...");
    }
}
