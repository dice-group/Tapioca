/**
 * tapioca.analyzer - ${project.description}
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
 * This file is part of tapioca.analyzer.
 *
 * tapioca.analyzer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.analyzer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.analyzer.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.analyzer.dump;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;

import org.aksw.simba.tapioca.extraction.Extractor;
import org.aksw.simba.tapioca.extraction.RDF2ExtractionStreamer;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDumpExtractorApplier {

	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDumpExtractorApplier.class);

	private static final String FILE_ENDING_COMPRESSION_MAPPING[][] = new String[][] {
			{ ".gz", CompressorStreamFactory.GZIP }, { ".bz2", CompressorStreamFactory.BZIP2 } };

	protected ExecutorService executor = null;
	protected CompressorStreamFactory compStreamFactory = new CompressorStreamFactory();

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
		InputStream in = null;
		try {
			in = new FileInputStream(dump);
			int compressionMethod = getCompressionMethod(dump);
			while (compressionMethod >= 0) {
				in = compStreamFactory.createCompressorInputStream(
						FILE_ENDING_COMPRESSION_MAPPING[compressionMethod][1], in);
				dump = dump.substring(0, dump.length() - FILE_ENDING_COMPRESSION_MAPPING[compressionMethod][0].length());
				compressionMethod = getCompressionMethod(dump);
			}
			// if (dump.endsWith(".gz")) {
			// in = new GZIPInputStream(in);
			// dump = dump.substring(0, dump.length() - 3);
			// }
			return streamer.runExtraction(in, "", RDFLanguages.resourceNameToLang(dump), extractors);
		} catch (Exception e) {
			LOGGER.error("Couldn't read dump file \"" + dump + "\". Ignoring this dump.", e);
			return false;
		} finally {
			IOUtils.closeQuietly(in);
		}
	}

	protected int getCompressionMethod(String fileName) {
		for (int i = 0; i < FILE_ENDING_COMPRESSION_MAPPING.length; ++i) {
			if (fileName.endsWith(FILE_ENDING_COMPRESSION_MAPPING[i][0])) {
				return i;
			}
		}
		return -1;
	}
}
