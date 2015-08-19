package org.aksw.simba.tapioca.analyzer.dump;

import java.util.Arrays;

import org.aksw.simba.topicmodeling.concurrent.tasks.Task;

public class DumpAnalyzingTask implements Task {

	protected String datasetName;
	protected String dumps[];

	@Override
	public void run() {
		// FIXME anaylze dumps
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
