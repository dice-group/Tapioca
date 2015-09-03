package org.aksw.simba.tapioca.analyzer.dump;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.zip.GZIPInputStream;

import org.aksw.simba.tapioca.extraction.Extractor;
import org.aksw.simba.tapioca.extraction.RDF2ExtractionStreamer;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDumpExtractorApplier {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDumpExtractorApplier.class);

    protected ExecutorService executor = null;

    public AbstractDumpExtractorApplier(ExecutorService executor) {
        this.executor = executor;
    }

    protected boolean extractFromDump(String dump, Extractor... extractors) {
        RDF2ExtractionStreamer streamer;
        if (executor != null) {
            streamer = new RDF2ExtractionStreamer(executor);
        } else {
            streamer = new RDF2ExtractionStreamer();
        }
        InputStream fin = null;
        try {
            fin = new FileInputStream(dump);
            if (dump.endsWith(".gz")) {
                fin = new GZIPInputStream(fin);
                dump = dump.substring(0, dump.length() - 3);
            }
            return streamer.runExtraction(fin, "", RDFLanguages.resourceNameToLang(dump), extractors);
        } catch (Exception e) {
            LOGGER.error("Couldn't read dump file \"" + dump + "\". Ignoring this dump.", e);
            return false;
        } finally {
            IOUtils.closeQuietly(fin);
        }
    }
}
