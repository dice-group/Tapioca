package org.aksw.simba.tapioca.analyzer.sparql;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.simba.tapioca.analyzer.label.LabelExtractionUtils;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.RDFClientLabelRetriever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class SPARQLEndpointLabelExtractor {

	private static final Logger LOGGER = LoggerFactory.getLogger(SPARQLEndpointLabelExtractor.class);

	private static final Set<String> LABEL_PROPERTIES = new HashSet<String>(
			Arrays.asList(RDFClientLabelRetriever.NAMING_PROPERTIES));

	private static final String PREDICATE = "p";
	private static final String OBJECT = "o";
	private static final String URI_REPLACEMENT = "%URI%";
	private static final String FROM_CLAUSE_REPLACEMENT = "%FROM%";
	private static final String LIST_TRIPLES_QUERY = "SELECT ?" + PREDICATE + " ?" + OBJECT + " "
			+ FROM_CLAUSE_REPLACEMENT + " WHERE { <" + URI_REPLACEMENT + "> ?" + PREDICATE + " ?" + OBJECT + " }";

	// private static final String OUTPUT_FOLDER = "sparql_endpoints_labels";
	// private static final String LABELS_FILE_ENDING = ".labels.object";
	// private static final int MAX_NUMBER_OF_WORKERS = 4;
	//
	// public static void main(String[] args) throws IOException {
	// String data[][] = VoidExtractor.readEndpointsFile(new
	// File(VoidExtractor.ENDPOINTS_FILE));
	// String endpoints[] = data[0];
	// String names[] = data[1];
	// String graphs[] = data[2];
	//
	// Map<EndpointConfig, String> filteredEndpoints =
	// VoidExtractor.filterEndpoints(endpoints, names, graphs);
	//
	// Overseer overseer = new BlockingOverseer(MAX_NUMBER_OF_WORKERS);
	// Reporter reporter = new LogReporter(overseer);
	// reporter = new RegularReporter(reporter, 600000);
	//
	// File voidFile, outputFile;
	// String endpointName;
	// for (EndpointConfig endpointCfg : filteredEndpoints.keySet()) {
	// endpointName = filteredEndpoints.get(endpointCfg);
	// voidFile = new File(VoidExtractor.OUTPUT_FOLDER + File.separator
	// + endpointName);
	// outputFile = new File(OUTPUT_FOLDER + File.separator
	// + endpointName + LABELS_FILE_ENDING);
	// if ((voidFile.exists()) && (!outputFile.exists())) {
	// overseer.startTask(new LabelExtractionTask(endpointCfg, voidFile,
	// outputFile));
	// }
	// }
	// }

	public static String[][] requestLabels(Set<String> uris, EndpointConfig endpointCfg) {
		QueryExecutionFactory qef = null;
		try {
			qef = SPARQLEndpointAnalyzer.initQueryExecution(endpointCfg);
		} catch (Exception e) {
			LOGGER.error("Couldn't create QueryExecutionFactory. Returning null. Exception: {}",
					e.getLocalizedMessage());
			return null;
		}
		String fromClause;
		if (endpointCfg.graph == null) {
			fromClause = "";
		} else {
			fromClause = "FROM <" + endpointCfg.graph + ">";
		}

		LOGGER.info("Requesting labels from " + endpointCfg + " ...");
		String queryForEndpoint = LIST_TRIPLES_QUERY.replace(FROM_CLAUSE_REPLACEMENT, fromClause);
		Set<String> labelsForUri;
		Map<String, Set<String>> labels = new HashMap<String, Set<String>>();
		try {
			for (String uri : uris) {
				labelsForUri = queryLabels(qef, queryForEndpoint.replace(URI_REPLACEMENT, uri));
				if ((labelsForUri != null) && (labelsForUri.size() > 0)) {
					labels.put(uri, labelsForUri);
				}
			}
		} catch (IOException e) {
			LOGGER.error("Couldn't query labels. Returning null.", e);
			return null;
		}
		if (labels.isEmpty()) {
			LOGGER.warn("Couldn't get any labels for the dataset \"" + endpointCfg + "\". Returning null.");
			return null;
		}
		LOGGER.info("Found labels for {} of the {} URIs.", labels.size(), uris.size());

		return LabelExtractionUtils.generateArray(labels);
	}

	private static Set<String> queryLabels(QueryExecutionFactory qef, String query) throws IOException {
		QueryExecution exec = qef.createQueryExecution(query);
		ResultSet resultSet = null;
		try {
			resultSet = exec.execSelect();
		} catch (Exception e) {
			throw new IOException("Couldn't query labels.", e);
		}
		Set<String> labels = new HashSet<String>();
		QuerySolution result = null;
		RDFNode predicate, object;
		while (resultSet.hasNext()) {
			result = resultSet.next();
			predicate = result.get(PREDICATE);
			if ((predicate != null) && (LABEL_PROPERTIES.contains(predicate.toString()))) {
				object = result.get(OBJECT);
				if (object != null) {
					labels.add(object.toString());
				}
			}
		}
		return labels;
	}

}
