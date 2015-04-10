package org.aksw.simba.tapioca.preprocessing.labelretrieving;

import java.util.List;

public interface TokenizedLabelRetriever {

    public List<String> getTokenizedLabel(String uri, String namespace);
}
