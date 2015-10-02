/**
 * This file is part of tapioca.analyzer.
 *
 * tapioca.analyzer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.analyzer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.analyzer.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.analyzer.sparql;

import java.sql.SQLException;

import org.aksw.jena_sparql_api.cache.core.QueryExecutionFactoryCacheEx;
import org.aksw.jena_sparql_api.cache.extra.CacheBackend;
import org.aksw.jena_sparql_api.cache.extra.CacheFrontend;
import org.aksw.jena_sparql_api.cache.extra.CacheFrontendImpl;
import org.aksw.jena_sparql_api.cache.h2.CacheCoreH2;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.delay.core.QueryExecutionFactoryDelay;
import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.aksw.jena_sparql_api.pagination.core.QueryExecutionFactoryPaginated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSPARQLClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSPARQLClient.class);

	/**
	 * The delay that the system will have between sending two queries.
	 */
	private static final int DELAY = 5000;

	/**
	 * The lifetime of the cache.
	 */
	private static final long CACHE_TIME_TO_LIVE = 31l * 24l * 60l * 60l * 1000l; // 1month

	private String cacheDirectory;

	public AbstractSPARQLClient() {
	}

	public AbstractSPARQLClient(String cacheDirectory) {
		this.cacheDirectory = cacheDirectory;
	}

	protected QueryExecutionFactory initQueryExecution(EndpointConfig endpointCfg) throws ClassNotFoundException,
			SQLException {
		QueryExecutionFactory qef;
		if (endpointCfg.graph != null) {
			qef = new QueryExecutionFactoryHttp(endpointCfg.uri, endpointCfg.graph);
		} else {
			qef = new QueryExecutionFactoryHttp(endpointCfg.uri);
		}

		qef = new QueryExecutionFactoryDelay(qef, DELAY);

		if (cacheDirectory != null) {
			CacheBackend cacheBackend = CacheCoreH2.create(true, cacheDirectory,
					endpointCfg.uri.replaceAll("[:/]", "_"), CACHE_TIME_TO_LIVE, true);
			CacheFrontend cacheFrontend = new CacheFrontendImpl(cacheBackend);
			qef = new QueryExecutionFactoryCacheEx(qef, cacheFrontend);
		} else {
			LOGGER.info("The cache directory has not been set. Creating an uncached SPARQL client.");
		}

		// qef = new QueryExecutionFactoryPaginated(qef, 100);
		try {
			return new QueryExecutionFactoryPaginated(qef, 100);
		} catch (Exception e) {
			LOGGER.warn("Couldn't create Factory with pagination. Returning Factory without pagination. Exception: {}",
					e.getLocalizedMessage());
			return qef;
		}
	}
}
