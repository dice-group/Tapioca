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
        this.language = language;
    }

    public void run() {
        // Call the parsing process.
        RDFDataMgr.parse(rdfStream, is, baseUri, language);
        LOGGER.debug("Finished streaming.");
    }

    public String getId() {
        return "InputStream2RDFStreamingTask";
    }

    public String getProgress() {
        return "Streaming.";
    }
    
}
