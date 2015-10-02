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

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import org.aksw.simba.topicmodeling.concurrent.tasks.Task;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

public class DumpAnalyzingTask implements Task {

	private static final Logger LOGGER = LoggerFactory.getLogger(DumpAnalyzingTask.class);

	protected String datasetURI;
	protected File outputFolder;
	// FIXME instead of strings we should use a simple strucutre that is able to
	// handle the media-type, charset and additional features
	protected String dumps[];
	protected DumpFileAnalyzer analyzer;

	public DumpAnalyzingTask(String datasetURI, File outputFolder, String dumps[]) {
		this(datasetURI, outputFolder, dumps, null);
	}

	public DumpAnalyzingTask(String datasetURI, File outputFolder, String dumps[], ExecutorService executor) {
		this.datasetURI = datasetURI;
		this.outputFolder = outputFolder;
		this.dumps = dumps;
		if (executor != null) {
			analyzer = new DumpFileAnalyzer(executor);
		} else {
			analyzer = new DumpFileAnalyzer();
		}
	}

	@Override
	public void run() {
		Model model = analyzer.extractVoidInfo(datasetURI, dumps);
		if (model != null) {
			writeModel(model);
		}
	}

	@Override
	public String getId() {
		return "Analyzing(" + datasetURI + ' ' + Arrays.toString(dumps) + ")";
	}

	@Override
	public String getProgress() {
		return "Analyzing dumps...";
	}

	private void writeModel(Model voidModel) {
		FileOutputStream fout = null;
		try {
			fout = new FileOutputStream(outputFolder + File.separator + "void.ttl");
			RDFDataMgr.write(fout, voidModel, Lang.TTL);
		} catch (Exception e) {
			LOGGER.error("", e);
		} finally {
			IOUtils.closeQuietly(fout);
		}
	}
}
