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
import org.aksw.simba.topicmodeling.lang.Term;
import org.aksw.simba.topicmodeling.lang.postagging.PosTaggingTermFilter;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyEditingDocumentSupplierDecorator;

import com.carrotsearch.hppc.ObjectArrayList;

public class SimpleTokenizedTextTermFilter extends
        AbstractPropertyEditingDocumentSupplierDecorator<SimpleTokenizedText> {

    protected PosTaggingTermFilter filter;

    public SimpleTokenizedTextTermFilter(DocumentSupplier documentSource, PosTaggingTermFilter filter) {
        super(documentSource, SimpleTokenizedText.class);
        this.filter = filter;
    }

    @Override
    protected void editDocumentProperty(SimpleTokenizedText property) {
        String tokens[] = property.getTokens();
        ObjectArrayList<String> newTokens = new ObjectArrayList<String>(tokens.length);
        // OpenTerm term = new OpenTerm();
        for (int i = 0; i < tokens.length; ++i) {
            // term.setWordForm(tokens[i]);
            // if (filter.isTermGood(term)) {
            if (filter.isTermGood(new Term(tokens[i]))) {
                newTokens.add(tokens[i]);
            }
        }
        property.setTokens(newTokens.toArray(String.class));
    }

    // private static class OpenTerm extends Term {
    //
    // private static final long serialVersionUID = 1L;
    //
    // protected String term;
    //
    // public OpenTerm() {
    // super("");
    // }
    //
    // @Override
    // public String getWordForm() {
    // return term;
    // }
    // s
    // @Override
    // public String getLemma() {
    // return term;
    // }
    //
    // public void setWordForm(String wordForm) {
    // this.term = wordForm;
    // }
    // }
}
