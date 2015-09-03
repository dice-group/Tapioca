package org.aksw.simba.tapioca.analyzer.sparql;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.commons.util.StreamUtils;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.simba.tapioca.data.vocabularies.VOID;
import org.aksw.simba.tapioca.extraction.voidex.VoidExtractor;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.vocabulary.RDF;

public class SPARQLEndpointAnalyzer extends AbstractSPARQLClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(SPARQLEndpointAnalyzer.class);

	private static final String VOCABULARY_BLACKLIST_FILE = "vocabulary_blacklist.txt";
	private static final String SPECIAL_CLASSES_LIST_FILE = "special_classes.txt";

	/**
	 * Result name of list result.
	 */
	private static final String LIST_NAME = "LIST";
	/**
	 * Result name of count result.
	 */
	private static final String COUNT_NAME = "COUNT";
	/**
	 * String marking the place of a certain URI inside of a query.
	 */
	private static final String URI_REPLACEMENT = "%URI%";
	/**
	 * String marking the position of the "FROM" clause inside a query.
	 */
	private static final String FROM_CLAUSE_REPLACEMENT = "%FROM%";
	/**
	 * Query for selecting the list of classes.
	 */
	private static final String CLASS_LIST_QUERY = "SELECT DISTINCT ?" + LIST_NAME + " " + FROM_CLAUSE_REPLACEMENT
			+ " WHERE { ?a <" + RDF.type.getURI() + "> ?" + LIST_NAME + " }";
	/**
	 * Projection part of a SPARQL 1.1 query that counts the number of triples
	 * selected by one of the "WHERE" clauses.
	 */
	private static final String SPARQL_11_COUNT_QUERY = "SELECT (COUNT(*) AS ?" + COUNT_NAME + ") "
			+ FROM_CLAUSE_REPLACEMENT + " WHERE ";
	/**
	 * Projection part of a SPARQL 1.0 query that retrieves all triples selected
	 * by one of the "WHERE" clauses. In difference to the preferred
	 * {@link #SPARQL_11_COUNT_QUERY}, this query does not count directly. Thus,
	 * the program code has to count the returned triples.
	 */
	private static final String SPARQL_10_COUNT_QUERY = "SELECT *  " + FROM_CLAUSE_REPLACEMENT + " WHERE ";
	/**
	 * Where clause that can be used to count the number of entities of a given
	 * type.
	 */
	private static final String COUNT_CLASS_WHERE_CLAUSE = "{ ?a <" + RDF.type.getURI() + "> <" + URI_REPLACEMENT
			+ "> }";
	/**
	 * Query for selecting the list of properties.
	 */
	private static final String PROPERTY_LIST_QUERY = "SELECT DISTINCT ?" + LIST_NAME + " " + FROM_CLAUSE_REPLACEMENT
			+ " WHERE { ?a ?" + LIST_NAME + " ?b }";
	/**
	 * Where clause that can be used to count the number of triples containing a
	 * given property.
	 */
	private static final String COUNT_PROPERTY_WHERE_CLAUSE = "{ ?a <" + URI_REPLACEMENT + "> ?b }";
	/**
	 * Query that retrieves a list of instances of a given class, e.g.,
	 * SKOS:Concept.
	 */
	private static final String REQUEST_ENTITIES_QUERY = "SELECT ?" + LIST_NAME + " " + FROM_CLAUSE_REPLACEMENT
			+ " WHERE { ?" + LIST_NAME + " <" + RDF.type.getURI() + "> <" + URI_REPLACEMENT + "> }";

	public SPARQLEndpointAnalyzer() {
	}

	public SPARQLEndpointAnalyzer(String cacheDirectory) {
		super(cacheDirectory);
	}

	public Model extractVoidInfo(EndpointConfig endpointCfg) {
		QueryExecutionFactory qef = null;
		try {
			qef = initQueryExecution(endpointCfg);
		} catch (Exception e) {
			LOGGER.error("Couldn't create QueryExecutionFactory. Aborting. Exception: {}", e.getLocalizedMessage());
			return null;
		}

		Set<String> specialClasses = loadSpecialClassesList();

		String fromClause;
		if (endpointCfg.graph == null) {
			fromClause = "";
		} else {
			fromClause = "FROM <" + endpointCfg.graph + ">";
		}

		LOGGER.info("Requesting classes from " + endpointCfg + " ...");
		Resource resources[] = queryList(qef, CLASS_LIST_QUERY.replace(FROM_CLAUSE_REPLACEMENT, fromClause));
		if (resources == null) {
			if (fromClause.isEmpty()) {
				LOGGER.error("Couldn't get class list. Returning null.");
				return null;
			} else {
				LOGGER.warn("Got no result. Retrying it without FROM clause...");
				resources = queryList(qef, CLASS_LIST_QUERY.replace(FROM_CLAUSE_REPLACEMENT, ""));
				if (resources == null) {
					LOGGER.error("Couldn't get class list. Returning null.");
					return null;
				} else {
					// there is a problem with the from clause. use an empty
					// one.
					fromClause = "";
				}
			}
		}
		LOGGER.info("Requesting class counts from " + endpointCfg + " ...");
		long counts[] = null;
		try {
			counts = queryCounts(qef, fromClause, COUNT_CLASS_WHERE_CLAUSE, resources);
		} catch (Exception e) {
			LOGGER.error("Exception while requesting class clounts from " + endpointCfg + ". returning null.", e);
			return null;
		}
		LOGGER.info("Checking special classes for " + endpointCfg + " ...");
		List<Resource[]> specialClassInstances = new ArrayList<Resource[]>();
		Resource instances[];
		for (int i = 0; i < resources.length; ++i) {
			if (specialClasses.contains(resources[i].getURI())) {
				instances = queryList(qef, REQUEST_ENTITIES_QUERY.replaceAll(URI_REPLACEMENT, resources[i].getURI())
						.replace(FROM_CLAUSE_REPLACEMENT, fromClause));
				if (instances == null) {
					LOGGER.warn("Couldn't get instances of \"{}\".", resources[i].getURI());
				}
				specialClassInstances.add(instances);
			}
		}

		Model voidModel = ModelFactory.createDefaultModel();
		Resource endpointAsResource = new ResourceImpl(endpointCfg.graph == null ? endpointCfg.uri : endpointCfg.graph);

		voidModel.add(endpointAsResource, RDF.type, VOID.Dataset);

		long entities = 0;
		Resource blank;
		for (int i = 0; i < resources.length; ++i) {
			blank = new ResourceImpl();
			voidModel.add(endpointAsResource, VOID.classPartition, blank);
			voidModel.add(blank, VOID.clazz, resources[i]);
			if (counts != null) {
				voidModel.addLiteral(blank, VOID.entities, counts[i]);
				entities += counts[i];
			}
		}

		for (int i = 0; i < specialClassInstances.size(); ++i) {
			instances = specialClassInstances.get(i);
			if (instances != null) {
				for (int j = 0; j < instances.length; ++j) {
					if (!voidModel.containsResource(instances[j])) {
						blank = new ResourceImpl();
						voidModel.add(endpointAsResource, VOID.classPartition, blank);
						voidModel.add(blank, VOID.clazz, instances[j]);
						voidModel.addLiteral(blank, VOID.entities, 0);
					}
				}
			}
		}

		voidModel.addLiteral(endpointAsResource, VOID.classes, resources.length);
		if (counts != null) {
			voidModel.addLiteral(endpointAsResource, VOID.entities, entities);
		}

		LOGGER.info("Requesting properties from " + endpointCfg + " ...");
		resources = queryList(qef, PROPERTY_LIST_QUERY.replace(FROM_CLAUSE_REPLACEMENT, fromClause));
		if (resources == null) {
			LOGGER.error("Couldn't get properties list. Returning null.");
			return null;
		}
		LOGGER.info("Requesting property counts from " + endpointCfg + " ...");
		counts = null;
		try {
			counts = queryCounts(qef, fromClause, COUNT_PROPERTY_WHERE_CLAUSE, resources);
		} catch (Exception e) {
			LOGGER.error("Exception while requesting class property counts from " + endpointCfg + ". returning null.",
					e);
			return null;
		}
		// long subjectCounts[] = null;
		// subjectCounts = queryCounts(qef[QUERY_FACTORY],
		// COUNT_PROPERTY_SUBJECTS_QUERY, resources);
		// long objectCounts[] = null;
		// objectCounts = queryCounts(qef[QUERY_FACTORY],
		// COUNT_PROPERTY_OBJECTS_QUERY, resources);

		int triples = 0;
		for (int i = 0; i < resources.length; ++i) {
			blank = new ResourceImpl();
			voidModel.add(endpointAsResource, VOID.propertyPartition, blank);
			voidModel.add(blank, VOID.property, resources[i]);
			// if (subjectCounts != null) {
			// voidModel.addLiteral(blank, VOID.distinctSubjects,
			// subjectCounts[i]);
			// }
			// if (objectCounts != null) {
			// voidModel.addLiteral(blank, VOID.distinctObjects,
			// objectCounts[i]);
			// }
			if (counts != null) {
				voidModel.addLiteral(blank, VOID.triples, counts[i]);
				triples += counts[i];
			}
		}
		if ((entities == 0) && (triples == 0)) {
			LOGGER.error("Got an empty VOID model without an entity and a triple. Returning null.");
			return null;
		}
		voidModel.addLiteral(endpointAsResource, VOID.properties, resources.length);
		if (counts != null) {
			voidModel.addLiteral(endpointAsResource, VOID.triples, triples);
		}
		return voidModel;
	}

	/**
	 * This method queries a list of resources using the given query and the
	 * given query execution factory. The result is returned as Resource array
	 * or null if an error occurred.
	 * 
	 * @param qef
	 * @param query
	 * @return
	 */
	private Resource[] queryList(QueryExecutionFactory qef, String query) {
		QueryExecution exec = qef.createQueryExecution(query);
		ResultSet resultSet = null;
		try {
			resultSet = exec.execSelect();
		} catch (Exception e) {
			LOGGER.error("Couldn't query list. Returning null for query \"" + query + "\".", e);
			return null;
		}
		List<Resource> results = new ArrayList<Resource>();
		QuerySolution result = null;
		while (resultSet.hasNext()) {
			try {
				result = resultSet.next();
				results.add(result.getResource(LIST_NAME));
			} catch (ClassCastException e) {
				RDFNode node = result.get(LIST_NAME);
				LOGGER.error("One of the results is not a resource as which it has been expected. "
						+ node.getClass().getSimpleName() + "(\"" + node.toString()
						+ "\"). Trying to create a resource from it.");
				results.add(new ResourceImpl(result.get(LIST_NAME).toString()));
			}
		}
		return results.toArray(new Resource[results.size()]);
	}

	/**
	 * This method creates a list of counts of the given Resources. Those
	 * resources can be either classes or properties - this depends on the given
	 * where clause. The method will try to get the counts using SPARQL 1.1. If
	 * this fails, it falls back to SPARQL 1.0.
	 * 
	 * @param qef
	 * @param fromClause
	 * @param whereClause
	 * @param list
	 * @return a list of counts or null if an error occurred.
	 */
	private long[] queryCounts(QueryExecutionFactory qef, String fromClause, String whereClause, Resource list[]) {
		long counts[] = new long[list.length];
		QueryExecution exec;
		String query10 = SPARQL_10_COUNT_QUERY + whereClause;
		String query11 = SPARQL_11_COUNT_QUERY + whereClause;
		String localQuery = null;
		ResultSet resultSet;
		boolean understandsSPARQL11 = true;
		try {
			for (int i = 0; i < counts.length; ++i) {
				if (understandsSPARQL11) {
					try {
						localQuery = query11.replaceAll(FROM_CLAUSE_REPLACEMENT, fromClause);
						localQuery = localQuery.replaceAll(URI_REPLACEMENT, list[i].getURI());
						exec = qef.createQueryExecution(localQuery);
						if (exec == null) {
							throw new NullPointerException("Couldn't create query execution object.");
						}
						resultSet = exec.execSelect();
						if (resultSet.hasNext()) {
							counts[i] = resultSet.next().getLiteral(COUNT_NAME).getLong();
						}
					} catch (Exception e) {
						LOGGER.warn("Couldn't execute SPARQL 1.1 query. Trying SPARQL 1.0. query:\n" + localQuery
								+ "\n", e);
						--i;
						understandsSPARQL11 = false;
					}
				} else {
					localQuery = query10.replaceAll(FROM_CLAUSE_REPLACEMENT, fromClause);
					localQuery = localQuery.replaceAll(URI_REPLACEMENT, list[i].getURI());
					exec = qef.createQueryExecution(localQuery);
					if (exec == null) {
						throw new NullPointerException("Couldn't create query execution object.");
					}
					resultSet = exec.execSelect();
					while (resultSet.hasNext()) {
						resultSet.next();
						++counts[i];
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error("Couldn't execute query. Aborting. query:\n" + localQuery + "\n", e);
			return null;
		}
		return counts;
	}

	public static Set<String> loadVocabBlacklist() {
		return loadList(VOCABULARY_BLACKLIST_FILE);
	}

	public static Set<String> loadSpecialClassesList() {
		return loadList(SPECIAL_CLASSES_LIST_FILE);
	}

	protected static Set<String> loadList(String listName) {
		InputStream is = VoidExtractor.class.getClassLoader().getResourceAsStream(listName);
		if (is == null) {
			LOGGER.error("Couldn't load list from resources. Returning null.");
			return new HashSet<String>();
		}
		String content = StreamUtils.toStringSafe(is);
		if (content == null) {
			LOGGER.error("Couldn't load list from resources. Returning null.");
			return new HashSet<String>();
		}
		String lines[] = content.split("\n");
		Set<String> list = new HashSet<String>();
		for (int i = 0; i < lines.length; ++i) {
			lines[i] = lines[i].trim();
			if (lines[i].length() > 0) {
				list.add(lines[i]);
			}
		}
		return list;
	}

	@Deprecated
	protected static String[][] readEndpointsFile(File file) throws IOException {
		List<String> lines = FileUtils.readLines(file);
		List<String> endpoints = new ArrayList<String>(lines.size());
		List<String> endpointNames = new ArrayList<String>(lines.size());
		List<String> graphs = new ArrayList<String>(lines.size());
		String parts[];
		for (String line : lines) {
			if (!line.isEmpty()) {
				parts = line.split("\t");
				if (parts.length >= 2) {
					endpointNames.add(parts[0]);
					endpoints.add(parts[1]);
					graphs.add((parts.length >= 3) ? parts[2] : null);
				}
			}
		}
		return new String[][] { endpoints.toArray(new String[endpoints.size()]),
				endpointNames.toArray(new String[endpointNames.size()]), graphs.toArray(new String[graphs.size()]) };
	}

	@Deprecated
	protected static Map<EndpointConfig, String> filterEndpoints(String[] endpoints, String[] names, String[] graphs) {
		Map<EndpointConfig, String> endpointNameMapping = new HashMap<EndpointConfig, String>();
		String name;
		EndpointConfig endpointCfg;
		for (int i = 0; i < endpoints.length; ++i) {
			endpointCfg = new EndpointConfig(endpoints[i], graphs[i]);
			if (endpointNameMapping.containsKey(endpointCfg)) {
				name = endpointNameMapping.get(endpointCfg) + ',' + names[i];
			} else {
				name = names[i];
			}
			// filter kupkb as it created errors in the past
			if (!name.equals("kupkb")) {
				endpointNameMapping.put(endpointCfg, name);
			}
		}
		return endpointNameMapping;
	}

}
