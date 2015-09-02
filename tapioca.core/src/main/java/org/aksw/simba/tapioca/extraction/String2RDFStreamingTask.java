package org.aksw.simba.tapioca.extraction;

import java.io.StringReader;

import org.aksw.simba.topicmodeling.concurrent.tasks.Task;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class String2RDFStreamingTask implements Task {

    private static final Logger LOGGER = LoggerFactory.getLogger(String2RDFStreamingTask.class);

    private StreamRDF rdfStream;
    private StringReader reader;
    private String baseUri;
    private Lang language;

    public String2RDFStreamingTask(StreamRDF rdfStream, StringReader reader, String baseUri, Lang language) {
        this.rdfStream = rdfStream;
        this.reader = reader;
        this.baseUri = baseUri;
        this.language = language;
    }

    @Override
    public void run() {
        // Call the parsing process.
        RDFDataMgr.parse(rdfStream, reader, baseUri, language);
        LOGGER.debug("Finished streaming.");
    }

    @Override
    public String getId() {
        return "String2RDFStreamingTask";
    }

    @Override
    public String getProgress() {
        return "Streaming.";
    }
}
