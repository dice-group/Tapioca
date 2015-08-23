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
