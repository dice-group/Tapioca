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
package org.aksw.simba.tapioca.data;

import org.dice_research.topicmodeling.utils.doc.AbstractArrayContainingDocumentProperty;
import org.dice_research.topicmodeling.utils.doc.ParseableDocumentProperty;
import org.dice_research.topicmodeling.utils.doc.StringContainingDocumentProperty;

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
