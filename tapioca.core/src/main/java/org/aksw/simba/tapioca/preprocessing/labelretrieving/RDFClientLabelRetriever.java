/**
 * tapioca.core - ${project.description}
 * Copyright © 2015 Data Science Group (DICE) (michael.roeder@uni-paderborn.de)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.preprocessing.labelretrieving;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.message.BasicHeader;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.WebContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDFClientLabelRetriever extends AbstractTokenizedLabelRetriever implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RDFClientLabelRetriever.class);
    
    /**
     * timeout for a single HTTP request (in seconds).
     */
    private static final int REQUEST_TIMEOUT = 15;

    private CloseableHttpClient httpClient;
    
    public static final String NAMING_PROPERTIES[] = { "http://www.w3.org/2000/01/rdf-schema#label",
            "http://xmlns.com/foaf/0.1/nick",
            "http://purl.org/dc/elements/1.1/title",
            "http://purl.org/rss/1.0/title",
            "http://xmlns.com/foaf/0.1/name",
            "http://purl.org/dc/terms/title",
            "http://www.geonames.org/ontology#name",
            "http://xmlns.com/foaf/0.1/nickname",
            "http://swrc.ontoware.org/ontology#name",
            "http://sw.cyc.com/CycAnnotations_v1#label",
            "http://rdf.opiumfield.com/lastfm/spec#title",
            "http://www.proteinontology.info/po.owl#ResidueName",
            "http://www.proteinontology.info/po.owl#Atom",
            "http://www.proteinontology.info/po.owl#Element",
            "http://www.proteinontology.info/po.owl#AtomName",
            "http://www.proteinontology.info/po.owl#ChainName",
            "http://purl.uniprot.org/core/fullName",
            "http://purl.uniprot.org/core/title",
            "http://www.aktors.org/ontology/portal#has-title",
            "http://www.w3.org/2004/02/skos/core#prefLabel",
            "http://www.aktors.org/ontology/portal#name",
            "http://xmlns.com/foaf/0.1/givenName",
            "http://www.w3.org/2000/10/swap/pim/contact#fullName",
            "http://xmlns.com/foaf/0.1/surName",
            "http://swrc.ontoware.org/ontology#title",
            "http://swrc.ontoware.org/ontology#booktitle",
            "http://www.aktors.org/ontology/portal#has-pretty-name",
            "http://purl.uniprot.org/core/orfName",
            "http://purl.uniprot.org/core/name",
            "http://www.daml.org/2003/02/fips55/fips-55-ont#name",
            "http://www.geonames.org/ontology#alternateName",
            "http://purl.uniprot.org/core/locusName",
            "http://www.w3.org/2004/02/skos/core#altLabel",
            "http://creativecommons.org/ns#attributionName",
            "http://www.aktors.org/ontology/portal#family-name",
            "http://www.aktors.org/ontology/portal#full-name" };
    
    public RDFClientLabelRetriever() {
        // max connections part taken from org.apache.jena.riot.web.HttpOp
        String s = System.getProperty("http.maxConnections", "5");
        int max = Integer.parseInt(s);
        httpClient =  HttpClientBuilder.create()
            .useSystemProperties()
            .setRedirectStrategy(new LaxRedirectStrategy())
            .setMaxConnPerRoute(max)
            .setMaxConnTotal(10 * max)
            .setDefaultRequestConfig(
                    RequestConfig.custom()
                    .setConnectTimeout(REQUEST_TIMEOUT * 1000)
                    .setConnectionRequestTimeout(REQUEST_TIMEOUT * 1000)
                    .setSocketTimeout(REQUEST_TIMEOUT * 1000)
                    .build())
            .setDefaultHeaders(Arrays.asList(new  BasicHeader("Accept", WebContent.defaultRDFAcceptHeader)))
            .build();
    }

    @Override
    public List<String> getTokenizedLabel(String uri, String namespace) {
        Model model = ModelFactory.createDefaultModel();
        try {
            RDFParser.create()
                .source(uri)
                .base(uri)
                .httpClient(httpClient)
                .parse(model);
            //reader.read(model, uri);
        } catch (Exception e) {
            return null;
        }
        Resource resource = model.getResource(uri);

        HashSet<String> names = new HashSet<String>();
        Property p;
        StmtIterator iterator;
        Statement s;
        String language;
        for (int i = 0; i < NAMING_PROPERTIES.length; ++i) {
            p = model.createProperty(NAMING_PROPERTIES[i]);
            iterator = resource.listProperties(p);
            while (iterator.hasNext()) {
                s = iterator.next();
                try {
                    language = s.getLanguage().toLowerCase();
                    if ((language.equals("en")) || (language.equals(""))) {
                        names.add(s.getString());
                    }
                } catch (Exception e) {
                    LOGGER.warn("Couldn't get language of literal for " + uri + ". Ignoring this literal.", e);
                }
            }
        }
        if (names.size() > 0) {
            return tokenize(names);
        } else {
            return null;
        }
    }

    private static List<String> tokenize(HashSet<String> names) {
        HashSet<String> uniqueLabels = new HashSet<String>();
        for (String label : names) {
            uniqueLabels.addAll(LabelTokenizerHelper.getSeparatedText(label));
        }
        return new ArrayList<String>(uniqueLabels);
    }

    // protected String requestEntityInfo(String uri) {
    // HttpClientParams parameters = new HttpClientParams();
    // parameters.setParameter("ACCEPT", "application/rdf");
    // HttpClient client = new HttpClient(parameters);
    // HttpMethod request = new GetMethod();
    // request.setFollowRedirects(true);
    // String response = null;
    // int status = 0;
    // try {
    // request.setURI(new URI(uri, true));
    // status = client.executeMethod(request);
    // response = request.getResponseBodyAsString();
    // } catch (Exception e) {
    // LOGGER.error("Exception while requesting \"" + uri + "\". Returning null.", e);
    // e.printStackTrace();
    // }
    // if ((status >= 200) && (status < 300)) {
    // return response;
    // } else {
    // LOGGER.error("Wrong status " + status + ". Returning null.");
    // return null;
    // }
    // }
    
    @Override
    public void close() throws IOException {
        httpClient.close();
    }

}
