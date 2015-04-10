package org.aksw.simba.tapioca.preprocessing.labelretrieving;

public interface TokenizedLabelRetrieverDecorator extends TokenizedLabelRetriever {

    public TokenizedLabelRetriever getDecoratedLabelRetriever();
}
