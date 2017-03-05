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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;

import org.aksw.simba.tapioca.cores.helper.Extractor;
import org.aksw.simba.tapioca.cores.helper.RDF2ExtractionStreamer;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Michael Roeder, Kai
 *
 */
public abstract class AbstractDumpExtractorApplier {
	
	/**
	 * Logging
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDumpExtractorApplier.class);
	
	/**
	 * Manage a service
	 */
	protected ExecutorService executor = null;
	
	/**
	 * Constructor
	 * @param executor Executor
	 */
	public AbstractDumpExtractorApplier(ExecutorService executor) {
		this.executor = executor;
	}
	
	/**
	 * Run the extraction
	 * @param inFile Input file
	 * @param extractors Extractors
	 * @return FALSE if the dump file can't be read
	 */
	protected boolean extractFromDump(String inFile, Extractor... extractors) {
		// create rdf extraction stream
		RDF2ExtractionStreamer streamer;
		
		// set executor
		if (executor != null) {
			streamer = new RDF2ExtractionStreamer(executor);
		} else {
			streamer = new RDF2ExtractionStreamer();
		}
		
		// create input stream
		InputStream inStream = null;
		
		// try extraction
		try {
			inStream = new FileInputStream( inFile );
			return streamer.runExtraction( inStream, "", RDFLanguages.resourceNameToLang( inFile ), extractors );
		}
		
		// handle exception
		catch( Exception e) {
			LOGGER.error("Couldn't read dump file \"" + inFile + "\". Ignoring this dump.", e);
			return false;
		}
		
		// quit smoothly
		finally {
			IOUtils.closeQuietly( inStream );
		}
	}

}
