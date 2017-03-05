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
package org.aksw.simba.tapioca.cores.preprocessing;

import org.aksw.simba.tapioca.cores.data.SimpleTokenizedText;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyAppendingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentTextWordIds;
import org.aksw.simba.topicmodeling.utils.vocabulary.Vocabulary;

public class SimpleWordIndexingSupplierDecorator extends
        AbstractPropertyAppendingDocumentSupplierDecorator<DocumentTextWordIds> {

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
            wordId = vocabulary.getId(tokens[i]);
            if (wordId == Vocabulary.WORD_NOT_FOUND) {
                vocabulary.add(tokens[i]);
                wordId = vocabulary.getId(tokens[i]);
            }
            wordIds[i] = wordId;
        }

        return new DocumentTextWordIds(wordIds);
    }
}
