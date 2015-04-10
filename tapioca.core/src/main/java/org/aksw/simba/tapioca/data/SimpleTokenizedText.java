package org.aksw.simba.tapioca.data;

import org.aksw.simba.topicmodeling.utils.doc.AbstractArrayContainingDocumentProperty;

public class SimpleTokenizedText extends AbstractArrayContainingDocumentProperty {

    private static final long serialVersionUID = 1L;

    private String tokens[];

    public SimpleTokenizedText() {
        this.tokens = new String[0];
    }

    public SimpleTokenizedText(String[] tokens) {
        this.tokens = tokens;
    }

    public Object[] getValueAsArray() {
        return tokens;
    }

    public String[] getTokens() {
        return tokens;
    }

    public void setTokens(String[] tokens) {
        this.tokens = tokens;
    }
}
