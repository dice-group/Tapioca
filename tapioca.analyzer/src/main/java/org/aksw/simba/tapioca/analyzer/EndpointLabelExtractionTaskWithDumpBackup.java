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
package org.aksw.simba.tapioca.analyzer;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.aksw.simba.tapioca.analyzer.dump.DumpLabelExtractionTask;
import org.aksw.simba.tapioca.analyzer.label.LabelExtractionUtils;
import org.aksw.simba.tapioca.analyzer.sparql.EndpointAnalyzingTask;
import org.aksw.simba.tapioca.analyzer.sparql.EndpointConfig;
import org.aksw.simba.tapioca.analyzer.sparql.SPARQLEndpointLabelExtractor;
import org.aksw.simba.topicmodeling.commons.io.StorageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndpointLabelExtractionTaskWithDumpBackup extends DumpLabelExtractionTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(EndpointAnalyzingTask.class);

	private EndpointConfig endpointCfg;
	private String cacheDirectory;
	private boolean analyzingEndpoint = true;

	public EndpointLabelExtractionTaskWithDumpBackup(EndpointConfig endpointCfg, File voidFile, File outputFile,
			String[] dumps, String cacheDirectory) {
		super(dumps, voidFile, outputFile);
		this.endpointCfg = endpointCfg;
		this.cacheDirectory = cacheDirectory;
	}

	public EndpointLabelExtractionTaskWithDumpBackup(EndpointConfig endpointCfg, File voidFile, File outputFile,
			String[] dumps, ExecutorService executor, String cacheDirectory) {
		super(dumps, voidFile, outputFile, executor);
		this.endpointCfg = endpointCfg;
		this.cacheDirectory = cacheDirectory;
	}

	@Override
	public void run() {
		LOGGER.info("Starting extraction from \"" + endpointCfg.uri + "\"...");
		try {
			if (outputFile.exists()) {
				LOGGER.info("There already is a file for \"" + endpointCfg.uri + "\". Jumping over this endpoint.");
				return;
			} else {
				Set<String> uris = LabelExtractionUtils.readUris(voidFile);
				if (uris == null) {
					return;
				}
				SPARQLEndpointLabelExtractor extractor = new SPARQLEndpointLabelExtractor(cacheDirectory);
				String labels[][] = extractor.requestLabels(uris, endpointCfg);
				if (labels != null) {
					StorageHelper.storeToFileSavely(labels, outputFile.getAbsolutePath());
					labels = null;
				} else {
					LOGGER.error("Error while requesting the void information of \"" + endpointCfg.uri
							+ "\". Trying to use the dump.");
					super.run();
				}
			}
		} catch (Exception e) {
			LOGGER.error("Error while requesting and storing the void information of \"" + endpointCfg.uri + "\".", e);
		} finally {
			LOGGER.info("Finished extraction from \"" + endpointCfg.uri + "\"...");
		}
	}

	@Override
	public String getProgress() {
		if (analyzingEndpoint) {
			return "analyzing endpoint...";
		} else {
			return super.getProgress();
		}
	}
}
