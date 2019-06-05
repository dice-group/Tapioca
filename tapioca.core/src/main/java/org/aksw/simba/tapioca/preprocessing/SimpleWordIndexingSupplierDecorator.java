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
package org.aksw.simba.tapioca.preprocessing;

import org.aksw.simba.tapioca.data.SimpleTokenizedText;
import org.dice_research.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyAppendingDocumentSupplierDecorator;
import org.dice_research.topicmodeling.utils.doc.Document;
import org.dice_research.topicmodeling.utils.doc.DocumentTextWordIds;
import org.dice_research.topicmodeling.utils.vocabulary.Vocabulary;

public class SimpleWordIndexingSupplierDecorator
        extends AbstractPropertyAppendingDocumentSupplierDecorator<DocumentTextWordIds> {

    private Vocabulary vocabulary;

    public SimpleWordIndexingSupplierDecorator(DocumentSupplier documentSource, Vocabulary vocabulary) {
        super(documentSource);
        this.vocabulary = vocabulary;
    }

    public Vocabulary getVocabulary() {
        return vocabulary;
    }

    @Override
    protected DocumentTextWordIds createPropertyForDocument(Document document) {
        SimpleTokenizedText tokenizedText = document.getProperty(SimpleTokenizedText.class);
        if ((tokenizedText == null) || (tokenizedText.getTokens().length == 0)) {
            return null;
        }

        String tokens[] = tokenizedText.getTokens();
        int wordIds[] = new int[tokens.length];
        int wordId;
        for (int i = 0; i < tokens.length; ++i) {
            synchronized (vocabulary) {
                wordId = vocabulary.getId(tokens[i]);
                if (wordId == Vocabulary.WORD_NOT_FOUND) {
                    vocabulary.add(tokens[i]);
                    wordId = vocabulary.getId(tokens[i]);
                }
            }
            wordIds[i] = wordId;
        }

        return new DocumentTextWordIds(wordIds);
    }
}
