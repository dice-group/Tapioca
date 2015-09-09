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
