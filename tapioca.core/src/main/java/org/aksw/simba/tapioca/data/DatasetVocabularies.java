package org.aksw.simba.tapioca.data;

import org.aksw.simba.topicmodeling.utils.doc.AbstractArrayContainingDocumentProperty;
import org.aksw.simba.topicmodeling.utils.doc.ParseableDocumentProperty;
import org.aksw.simba.topicmodeling.utils.doc.StringContainingDocumentProperty;

public class DatasetVocabularies extends AbstractArrayContainingDocumentProperty implements
        StringContainingDocumentProperty, ParseableDocumentProperty {

    private static final String DELIMITTER_WRITING = "|";
    private static final String DELIMITTER_PARSING = "\\|";

    private static final long serialVersionUID = 1L;

    private String vocabularies[];

    public DatasetVocabularies() {
    }

    public DatasetVocabularies(String[] vocabularies) {
        this.vocabularies = vocabularies;
    }

    public Object[] getValueAsArray() {
        return vocabularies;
    }

    public void parseValue(String value) {
        if (value.isEmpty()) {
            vocabularies = new String[0];
        } else {
            vocabularies = value.split(DELIMITTER_PARSING);
        }
    }

    public String getStringValue() {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (int i = 0; i < vocabularies.length; ++i) {
            if (first) {
                first = false;
            } else {
                result.append(DELIMITTER_WRITING);
            }
            result.append(vocabularies[i]);
        }
        return result.toString();
    }

    public String[] getVocabularies() {
        return vocabularies;
    }

}
