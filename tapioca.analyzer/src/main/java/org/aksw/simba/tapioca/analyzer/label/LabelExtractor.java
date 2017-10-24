/**
 * tapioca.analyzer - ${project.description}
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
package org.aksw.simba.tapioca.analyzer.label;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aksw.simba.tapioca.extraction.AbstractExtractor;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.RDFClientLabelRetriever;

import com.hp.hpl.jena.graph.Triple;

public class LabelExtractor extends AbstractExtractor {

    private static final Set<String> LABEL_PROPERTIES = new HashSet<String>(
            Arrays.asList(RDFClientLabelRetriever.NAMING_PROPERTIES));

    private Set<String> uris;
    private Map<String, Set<String>> labels;

    public LabelExtractor(Set<String> uris) {
        this(uris, new HashMap<String, Set<String>>());
    }

    public LabelExtractor(Set<String> uris, Map<String, Set<String>> labels) {
        this.uris = uris;
        this.labels = labels;
    }

    public void handleTriple(Triple triple) {
        if (LABEL_PROPERTIES.contains(triple.getPredicate().toString())) {
            String subject = triple.getSubject().toString();
            if (uris.contains(subject)) {
                Set<String> labelsOfUri;
                if (labels.containsKey(subject)) {
                    labelsOfUri = labels.get(subject);
                } else {
                    labelsOfUri = new HashSet<String>();
                    labels.put(subject, labelsOfUri);
                }
                labelsOfUri.add(triple.getObject().toString());
            }
        }
    }

    public Map<String, Set<String>> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, Set<String>> labels) {
        this.labels = labels;
    }
}
