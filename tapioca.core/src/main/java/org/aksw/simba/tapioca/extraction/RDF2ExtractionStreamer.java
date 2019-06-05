/**
 * tapioca.core - ${project.description}
 * Copyright © 2015 Data Science Group (DICE) (michael.roeder@uni-paderborn.de)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * This file is part of tapioca.core.
 *
 * tapioca.core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.core.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.extraction;

import java.io.InputStream;
import java.io.StringReader;
import java.util.concurrent.ExecutorService;

import org.dice_research.topicmodeling.concurrent.overseers.Overseer;
import org.dice_research.topicmodeling.concurrent.overseers.pool.ExecutorBasedOverseer;
import org.dice_research.topicmodeling.concurrent.overseers.simple.SimpleOverseer;
import org.dice_research.topicmodeling.concurrent.tasks.Task;
import org.dice_research.topicmodeling.concurrent.tasks.TaskObserver;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@Deprecated
//@NotThreadSafe
public class RDF2ExtractionStreamer implements TaskObserver {

    private static final Logger LOGGER = LoggerFactory.getLogger(RDF2ExtractionStreamer.class);

    // private ExecutorService executor;
    private Overseer overseer;
    private Thread streamerThread;

    public RDF2ExtractionStreamer() {
        // this.executor = Executors.newSingleThreadExecutor();
        overseer = new SimpleOverseer();
        overseer.addObserver(this);
    }

    public RDF2ExtractionStreamer(ExecutorService executor) {
        // this.executor = executor;
        overseer = new ExecutorBasedOverseer(executor);
        overseer.addObserver(this);
    }

    public boolean runExtraction(String rdfData, String baseUri, Lang language, Extractor... extractors) {
        PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
        PipedRDFStream<Triple> rdfStream = new PipedTriplesStream(iter);
        BlockingStreamRDFDecorator streamDecorator = new BlockingStreamRDFDecorator(rdfStream);
        return runExtraction(iter, streamDecorator,
                new String2RDFStreamingTask(streamDecorator, new StringReader(rdfData), baseUri, language), extractors);
    }

    public boolean runExtraction(InputStream is, String baseUri, Lang language, Extractor... extractors) {
        PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
        PipedRDFStream<Triple> rdfStream = new PipedTriplesStream(iter);
        BlockingStreamRDFDecorator streamDecorator = new BlockingStreamRDFDecorator(rdfStream);
        return runExtraction(iter, streamDecorator,
                new InputStream2RDFStreamingTask(streamDecorator, is, baseUri, language), extractors);
    }

    protected boolean runExtraction(PipedRDFIterator<Triple> iter, BlockingStreamRDFDecorator streamDecorator,
            Task task, Extractor extractors[]) {
        // executor.execute(task);
        streamerThread = Thread.currentThread();
        overseer.startTask(task);
        // Wait for the producer to start
        try {
            streamDecorator.waitToStart();
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting for the producer. Aborting.", e);
            return false;
        }
        runExtraction(iter, extractors);
        return true;
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

    @Override
    public void reportTaskFinished(Task task) {
        // nothing to do
    }

    @Override
    public void reportTaskThrowedException(Task task, Throwable t) {
        LOGGER.error("Streaming task throwed an exception. I will try to interrupt the stream reading thread.", t);
        streamerThread.interrupt();
    }
}
