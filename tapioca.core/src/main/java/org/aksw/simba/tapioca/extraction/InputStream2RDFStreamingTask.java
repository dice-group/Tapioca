package org.aksw.simba.tapioca.extraction;

import java.io.InputStream;

import org.aksw.simba.topicmodeling.concurrent.tasks.Task;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InputStream2RDFStreamingTask implements Task {

    private static final Logger LOGGER = LoggerFactory.getLogger(InputStream2RDFStreamingTask.class);

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
        LOGGER.debug("Finished streaming.");
    }

    @Override
    public String getId() {
        return "InputStream2RDFStreamingTask";
    }

    @Override
    public String getProgress() {
        return "Streaming.";
    }
}
