/**
 * This file is part of tapioca.cores.
 *
 * tapioca.cores is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.cores is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.cores.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.cores.extraction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aksw.simba.tapioca.cores.helper.AbstractExtractor;
import org.aksw.simba.tapioca.cores.helper.RDFClientLabelRetriever;

import com.hp.hpl.jena.graph.Triple;

/**
 * This class handles triples from RDF model an extracts
 * labels from it. It is kind of the core functionality
 * for label extraction
 * 
 * @author Michael Roeder, Chris
 *
 */
public class LabelExtractor extends AbstractExtractor {

	/**
	 * Set RDF properties about labels
	 */
    private static final Set<String> LABEL_PROPERTIES = new HashSet<String>(
            Arrays.asList(RDFClientLabelRetriever.NAMING_PROPERTIES));

    /**
     * Set of URIs to find labels for
     */
    private Set<String> uris;
    
    /**
     * KEY-VALUE store for URIs and their labels
     */
    private Map<String, Set<String>> labels;

    /**
     * Constructor
     * @param uris Set of URIs to find labels for
     */
    public LabelExtractor(Set<String> uris) {
        this(uris, new HashMap<String, Set<String>>());
    }
    
    /**
     * Constructor 
     * @param uris Set of URIs to find labels for
     * @param labels Label cache
     */
    public LabelExtractor(Set<String> uris, Map<String, Set<String>> labels) {
        this.uris = uris;
        this.labels = labels;
    }

	/**
	 * Method to extract metadata from a RDF triple 
	 * @param triple RDF triple
	 */
    public void handleTriple(Triple triple) {
    	// If the predicate tells something about labels, then...
        if (LABEL_PROPERTIES.contains(triple.getPredicate().toString())) {
        	// Get subject of the triple
            String subject = triple.getSubject().toString();
            
            // If subject is an URI we want to get a labels for, then... 
            if (uris.contains(subject)) {
                Set<String> labelsOfUri;
                
                // Do we have the labels already in the cache, then...
                if (labels.containsKey(subject)) {
                    labelsOfUri = labels.get(subject);
                }

                // If not in cache, then...
                else {
                    labelsOfUri = new HashSet<String>();
                    labels.put(subject, labelsOfUri);
                }
                
                // add labels to cache
                labelsOfUri.add( triple.getObject().toString( false ) );                
            }
        }
    }

    /**
     * Get method
     * @return Labels
     */
    public Map<String, Set<String>> getLabels() {
        return labels;
    }

    /**
     * Set method
     * @param labels Labels
     */
    public void setLabels(Map<String, Set<String>> labels) {
        this.labels = labels;
    }
    
}
