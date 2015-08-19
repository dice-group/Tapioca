package org.aksw.simba.tapioca.analyzer.dump;

import java.io.File;
import java.util.Arrays;

public class DumpLoadingTask extends DumpAnalyzingTask {

	protected File downloadFolder;
	protected boolean loadingFinished = false;

	@Override
	public void run() {
		loadingFinished = false;
		// FIXME load dumps
		
		loadingFinished = true;
		super.run();
	}

	@Override
	public String getId() {
		return "Loading/Analyzing(" + datasetName + ' ' + Arrays.toString(dumps) + ")";
	}

	@Override
	public String getProgress() {
		if (loadingFinished) {
			return "Analyzing dumps...";
		} else {
			return "Loading dumps...";
		}
	}
}
