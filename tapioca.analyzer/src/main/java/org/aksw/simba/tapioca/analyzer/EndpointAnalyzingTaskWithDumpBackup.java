package org.aksw.simba.tapioca.analyzer;

import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.ExecutorService;

import org.aksw.simba.tapioca.analyzer.dump.DumpLoadingTask;
import org.aksw.simba.tapioca.analyzer.sparql.EndpointAnalyzingTask;
import org.aksw.simba.tapioca.analyzer.sparql.EndpointConfig;
import org.aksw.simba.tapioca.analyzer.sparql.SPARQLEndpointAnalyzer;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

public class EndpointAnalyzingTaskWithDumpBackup extends DumpLoadingTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(EndpointAnalyzingTask.class);

	private EndpointConfig endpointCfg;
	private String cacheDirectory;
	private boolean analyzingEndpoint = true;

	public EndpointAnalyzingTaskWithDumpBackup(EndpointConfig endpointCfg, String datasetURI, File outputFolder,
			String[] dumps, String cacheDirectory) {
		super(datasetURI, outputFolder, dumps);
		this.endpointCfg = endpointCfg;
		this.cacheDirectory = cacheDirectory;
	}

	public EndpointAnalyzingTaskWithDumpBackup(EndpointConfig endpointCfg, String datasetURI, File outputFolder,
			String[] dumps, ExecutorService executor, String cacheDirectory) {
		super(datasetURI, outputFolder, dumps, executor);
		this.endpointCfg = endpointCfg;
		this.cacheDirectory = cacheDirectory;
	}

	@Override
	public void run() {
		LOGGER.info("Starting extraction from \"" + this.datasetURI + "\"...");
		SPARQLEndpointAnalyzer analyzer = new SPARQLEndpointAnalyzer(cacheDirectory);
		File outputFile = new File(outputFolder.getAbsolutePath() + File.separator + "void.ttl");
		try {
			if (outputFile.exists()) {
				LOGGER.info("There already is a file for \"" + this.datasetURI + "\". Jumping over this endpoint.");
				return;
			} else {
				Model voidModel = analyzer.extractVoidInfo(endpointCfg);
				if (voidModel != null) {
					voidModel.setNsPrefix("void", "http://rdfs.org/ns/void#");
					// NTripleWriter writer = new NTripleWriter();
					FileOutputStream fout = new FileOutputStream(outputFile);
					// writer.write(voidModel, fout, "");
					RDFDataMgr.write(fout, voidModel, RDFFormat.TURTLE_PRETTY);
					fout.close();
				} else {
					LOGGER.error("Error while requesting the void information of \"" + this.datasetURI
							+ "\". Trying to use the dump.");
					super.run();
				}
			}
		} catch (Exception e) {
			LOGGER.error("Error while requesting and storing the void information of \"" + this.datasetURI + "\".", e);
		} finally {
			LOGGER.info("Finished extraction from \"" + this.datasetURI + "\"...");
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
