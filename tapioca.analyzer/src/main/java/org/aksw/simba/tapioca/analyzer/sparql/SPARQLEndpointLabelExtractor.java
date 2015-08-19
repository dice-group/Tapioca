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

public class SPARQLEndpointLabelExtractor extends AbstractSPARQLClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(SPARQLEndpointLabelExtractor.class);

	private static final Set<String> LABEL_PROPERTIES = new HashSet<String>(
			Arrays.asList(RDFClientLabelRetriever.NAMING_PROPERTIES));

	private static final String PREDICATE = "p";
	private static final String OBJECT = "o";
	private static final String URI_REPLACEMENT = "%URI%";
	private static final String FROM_CLAUSE_REPLACEMENT = "%FROM%";
	private static final String LIST_TRIPLES_QUERY = "SELECT ?" + PREDICATE + " ?" + OBJECT + " "
			+ FROM_CLAUSE_REPLACEMENT + " WHERE { <" + URI_REPLACEMENT + "> ?" + PREDICATE + " ?" + OBJECT + " }";

	public SPARQLEndpointLabelExtractor() {
	}

	public SPARQLEndpointLabelExtractor(String cacheDirectory) {
		super(cacheDirectory);
	}

	public String[][] requestLabels(Set<String> uris, EndpointConfig endpointCfg) {
		QueryExecutionFactory qef = null;
		try {
			qef = initQueryExecution(endpointCfg);
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

	private Set<String> queryLabels(QueryExecutionFactory qef, String query) throws IOException {
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
