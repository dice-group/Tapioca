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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.dice_research.lodcat.api.Client;
import org.dice_research.lodcat.api.ResponseURIData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LODCatLabelServiceBasedRetriever extends AbstractTokenizedLabelRetriever {

    private static final Logger LOGGER = LoggerFactory.getLogger(LODCatLabelServiceBasedRetriever.class);

    protected Client client;

    public LODCatLabelServiceBasedRetriever() {
        client = new Client();
    }

    public LODCatLabelServiceBasedRetriever(String serviceUrl) {
        client = new Client(serviceUrl);
    }

    @Override
    public List<String> getTokenizedLabel(String uri, String namespace) {
        try {
            Map<String, ResponseURIData> response = client.getDetails(Arrays.asList(uri));
            return tokenize(response.get(uri).getLabels());
        } catch (Exception e) {
            LOGGER.warn("Couldn't get labels for " + uri + ". Returning null.", e);
            return null;
        }
    }

    private static List<String> tokenize(Collection<String> names) {
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
    // LOGGER.error("Exception while requesting \"" + uri + "\". Returning null.",
    // e);
    // e.printStackTrace();
    // }
    // if ((status >= 200) && (status < 300)) {
    // return response;
    // } else {
    // LOGGER.error("Wrong status " + status + ". Returning null.");
    // return null;
    // }
    // }

}
