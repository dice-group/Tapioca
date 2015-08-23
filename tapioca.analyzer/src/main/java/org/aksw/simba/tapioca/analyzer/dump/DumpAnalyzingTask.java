package org.aksw.simba.tapioca.analyzer.dump;

import java.io.File;
import java.util.Arrays;

import org.aksw.simba.topicmodeling.concurrent.tasks.Task;

public class DumpAnalyzingTask implements Task {

	protected String datasetName;
	protected String dumps[];
	protected File outputFile;

	@Override
	public void run() {
		// FIXME anaylze dump
		// TODO write VOID file
	}

	@Override
	public String getId() {
		return "Analyzing(" + datasetName + ' ' + Arrays.toString(dumps) + ")";
	}

	@Override
	public String getProgress() {
		return "Analyzing dumps...";
	}
}
