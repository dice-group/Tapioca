package org.aksw.simba.tapioca.preprocessing;

import org.aksw.simba.tapioca.data.SimpleTokenizedText;
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
