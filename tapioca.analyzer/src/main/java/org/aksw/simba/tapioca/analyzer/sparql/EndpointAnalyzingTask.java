package org.aksw.simba.tapioca.analyzer.sparql;

import java.io.File;
import java.io.FileOutputStream;

import org.aksw.simba.topicmodeling.concurrent.tasks.Task;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

public class EndpointAnalyzingTask implements Task {

	private static final Logger LOGGER = LoggerFactory.getLogger(EndpointAnalyzingTask.class);

	private EndpointConfig endpointCfg;
	private String endpointName;
	private File outputFile;

	public EndpointAnalyzingTask(String endpoint, File outputFile) {
		this(new EndpointConfig(endpoint), endpoint, outputFile);
	}

	public EndpointAnalyzingTask(EndpointConfig endpointCfg, String endpointName, File outputFile) {
		this.endpointCfg = endpointCfg;
		this.endpointName = endpointName;
		this.outputFile = outputFile;
	}

	@Override
	public void run() {
		LOGGER.info("Starting extraction from \"" + endpointName + "\"...");
		SPARQLEndpointAnalyzer analyzer = new SPARQLEndpointAnalyzer();
		try {
			if (outputFile.exists()) {
				LOGGER.info("There already is a file for \"" + endpointName + "\". Jumping over this endpoint.");
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
					LOGGER.error("Error while requesting the void information of \"" + endpointName + "\".");
				}
			}
		} catch (Exception e) {
			LOGGER.error("Error while requesting and storing the void information of \"" + endpointName + "\".", e);
		} finally {
			LOGGER.info("Finished extraction from \"" + endpointName + "\"...");
		}
	}

	@Override
	public String getId() {
		return endpointName;
	}

	@Override
	public String getProgress() {
		return "";
	}
}
