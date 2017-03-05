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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jena.riot.system.StreamRDF;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;

/**
 *
 */
public class BlockingStreamRDFDecorator implements StreamRDFDecorator {
    /**
     * The decorated stream.
     */
    private final StreamRDF stream;
    /**
     * A {@link Lock} used to secure the {@link #flagSet} condition.
     */
    private final Lock flagLock = new ReentrantLock();
    /**
     * A {@link Condition} used to make the consumer wait for the start signal.
     */
    private final Condition flagSet = flagLock.newCondition();
    /**
     * A flag used to store whether the stream already has started. It is needed
     * if the start signal occurs before the consumer entered the
     * {@link #waitToStart()} method.
     */
    private boolean started = false;

    /**
     * Constructor
     * @param stream A RDF stream
     */
    public BlockingStreamRDFDecorator(StreamRDF stream) {
        this.stream = stream;
    }

    public void start() {
        stream.start();
        started = true;
        flagLock.lock();
        try {
            flagSet.signalAll();
        } finally {
            flagLock.unlock();
        }
    }

    public void triple(Triple triple) {
        stream.triple(triple);
    }
    

    public void quad(Quad quad) {
        stream.quad(quad);
    }


    public void base(String base) {
        stream.base(base);
    }


    public void prefix(String prefix, String iri) {
        stream.prefix(prefix, iri);
    }


    public void finish() {
        stream.finish();
    }

    public StreamRDF getDecorated() {
        return stream;
    }

    /**
     * This methods blocks until the stream has started.
     * 
     * @throws InterruptedException An interrupt exception.
     */
    public void waitToStart() throws InterruptedException {
        while (!started) {
            flagLock.lock();
            try {
                flagSet.await();
            } finally {
                flagLock.unlock();
            }
        }
        
    }

}
