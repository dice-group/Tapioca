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
package org.aksw.simba.tapioca.analyzer.sparql;

import java.io.File;
import java.util.Set;

import org.aksw.simba.tapioca.analyzer.label.LabelExtractionUtils;
import org.aksw.simba.topicmodeling.commons.io.StorageHelper;
import org.aksw.simba.topicmodeling.concurrent.tasks.Task;

public class EndpointLabelExtractionTask implements Task {

	private EndpointConfig endpointCfg;
	private File voidFile;
	private File outputFile;
	private String cacheDirectory;

	public EndpointLabelExtractionTask(EndpointConfig endpointCfg, File voidFile, File outputFile) {
		this(endpointCfg, voidFile, outputFile, null);
	}

	public EndpointLabelExtractionTask(EndpointConfig endpointCfg, File voidFile, File outputFile, String cacheDirectory) {
		this.endpointCfg = endpointCfg;
		this.voidFile = voidFile;
		this.outputFile = outputFile;
		this.cacheDirectory = cacheDirectory;
	}

	@Override
	public void run() {
		// read URIs from void file
		Set<String> uris = LabelExtractionUtils.readUris(voidFile);
		if (uris == null) {
			return;
		}

		SPARQLEndpointLabelExtractor extractor = new SPARQLEndpointLabelExtractor(cacheDirectory);
		String labels[][] = extractor.requestLabels(uris, endpointCfg);
		if (labels != null) {
			StorageHelper.storeToFileSavely(labels, outputFile.getAbsolutePath());
			labels = null;
		}
	}

	@Override
	public String getId() {
		return endpointCfg.uri;
	}

	@Override
	public String getProgress() {
		return "";
	}

}
