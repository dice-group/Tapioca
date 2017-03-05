/**
 * This file is part of tapioca.cores.
 *
 * tapioca.cores is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.cores is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.cores.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.cores.helper;

import java.io.InputStream;
import java.util.concurrent.ExecutorService;

import org.aksw.simba.tapioca.cores.helper.InputStream2RDFStreamingTask;
import org.aksw.simba.tapioca.cores.helper.BlockingStreamRDFDecorator;
import org.aksw.simba.tapioca.cores.helper.Extractor;
import org.aksw.simba.topicmodeling.concurrent.overseers.Overseer;
import org.aksw.simba.topicmodeling.concurrent.overseers.pool.ExecutorBasedOverseer;
import org.aksw.simba.topicmodeling.concurrent.overseers.simple.SimpleOverseer;
import org.aksw.simba.topicmodeling.concurrent.tasks.Task;
import org.aksw.simba.topicmodeling.concurrent.tasks.TaskObserver;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Triple;

/**
 * Class to manage the streaming of RDF from a source (a String or an
 * InputStream) to a set of {@link Extractor} instances. Internally it is based
 * on at least one further thread that is used for the streaming itself (as
 * described in the Jena RDF streaming description). During the streaming the
 * {@link Extractor#handleTriple(Triple)} is called.
 * 
 * @author Michael Roeder
 *
 */
public class RDF2ExtractionStreamer implements TaskObserver {
	
	/**
	 * Logging
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(RDF2ExtractionStreamer.class);
	
	/**
	 * Service management
	 */
	private Overseer overseer;
	
    /**
     * Thread
     */
    private Thread streamerThread;
    
    /**
     * Constructor
     */
    public RDF2ExtractionStreamer() {
        overseer = new SimpleOverseer();
        overseer.addObserver(this);
    }
    
    /**
     * Constructor
     * @param executor Executor
     */
    public RDF2ExtractionStreamer(ExecutorService executor) {
        overseer = new ExecutorBasedOverseer(executor);
        overseer.addObserver(this);
    }

     /**
     * (Variation 1)
     * 
     * @param is Input stream
     * @param baseUri URI of data set
     * @param language Language of data set
     * @param extractors Extractor
     * @return TRUE or FALSE
     */
    public boolean runExtraction(InputStream is, String baseUri, Lang language, Extractor... extractors) {
    	// create iterator
        PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
        
        // create stream
        PipedRDFStream<Triple> rdfStream = new PipedTriplesStream(iter);
        
        // create decorator
        BlockingStreamRDFDecorator streamDecorator = new BlockingStreamRDFDecorator(rdfStream);
        
        // call function again
        return runExtraction(iter, streamDecorator,
                new InputStream2RDFStreamingTask(streamDecorator, is, baseUri, language), extractors);        
    }

    /**
     * (Variation 2)
     * @param iter Iterator
     * @param streamDecorator Stream decorator
     * @param task Task
     * @param extractors Extractors
     * @return TRUE or FALSE
     */
    protected boolean runExtraction(PipedRDFIterator<Triple> iter, BlockingStreamRDFDecorator streamDecorator,
            Task task, Extractor extractors[]) {
       	// management
        streamerThread = Thread.currentThread();
        overseer.startTask( task );
        
        // wait before execution
        try {
            streamDecorator.waitToStart();
        }
        catch ( InterruptedException e ) {
            LOGGER.error("Interrupted while waiting for the producer. Aborting.", e);
            return false;
        }
        
        // call function again
        runExtraction(iter, extractors);
        return true;
    }
    
     /**
     * (Variation 3)
     * 
     * @param iter Iterator for RDF triple
     * @param extractors Extractor
     */
    protected void runExtraction(PipedRDFIterator<Triple> iter, Extractor extractors[]) {
        Triple triple;
        
        // As long as we have a triple left, do..
        // (1) take a triple
        // (2) call "handleTriple()" on it
        while (iter.hasNext()) {
            triple = iter.next();
            for (int i = 0; i < extractors.length; ++i) {
                extractors[i].handleTriple(triple);
            }
        }
    }
    
    /**
     * Success case 
     */
    public void reportTaskFinished(Task task) {
        // nothing to do
    }

    /**
     * Error case
     */
    public void reportTaskThrowedException(Task task, Throwable t) {
        LOGGER.error("Streaming task throwed an exception. I will try to interrupt the stream reading thread.", t);
        streamerThread.interrupt();
    }


}
