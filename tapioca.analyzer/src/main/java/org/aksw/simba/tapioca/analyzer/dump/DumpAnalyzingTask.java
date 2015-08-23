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
	protected String datasetName;
	protected File outputFolder;
	protected String dumps[];
	protected DumpFileAnalyzer analyzer;

	public DumpAnalyzingTask(String datasetURI, String datasetName, File outputFolder, String dumps[]) {
		this(datasetURI, datasetName, outputFolder, dumps, null);
	}

	public DumpAnalyzingTask(String datasetURI, String datasetName, File outputFolder, String dumps[],
			ExecutorService executor) {
		this.datasetURI = datasetURI;
		this.datasetName = datasetName;
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
		writeModel(analyzer.extractVoidInfo(datasetURI, dumps));
	}

	@Override
	public String getId() {
		return "Analyzing(" + datasetName + ' ' + Arrays.toString(dumps) + ")";
	}

	@Override
	public String getProgress() {
		return "Analyzing dumps...";
	}

	private void writeModel(Model voidModel) {
		FileOutputStream fout = null;
		try {
			fout = new FileOutputStream(outputFolder + File.separator + datasetName + ".ttl");
			RDFDataMgr.write(fout, voidModel, Lang.TTL);
		} catch (Exception e) {
			LOGGER.error("", e);
		} finally {
			IOUtils.closeQuietly(fout);
		}
	}
}
