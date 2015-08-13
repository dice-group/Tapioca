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

	public EndpointLabelExtractionTask(EndpointConfig endpointCfg, File voidFile, File outputFile) {
		this.endpointCfg = endpointCfg;
		this.voidFile = voidFile;
		this.outputFile = outputFile;
	}

	@Override
	public void run() {
		// read URIs from void file
		Set<String> uris = LabelExtractionUtils.readUris(voidFile);
		if (uris == null) {
			return;
		}

		String labels[][] = SPARQLEndpointLabelExtractor.requestLabels(uris, endpointCfg);
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
