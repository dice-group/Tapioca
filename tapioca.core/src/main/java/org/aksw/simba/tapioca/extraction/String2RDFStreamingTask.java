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
