package org.aksw.simba.tapioca.data;

import org.aksw.simba.topicmodeling.utils.doc.AbstractStringArrayContainingDocumentProperty;

public class SimpleTokenizedText extends AbstractStringArrayContainingDocumentProperty {

	private static final long serialVersionUID = 1L;

	private String tokens[];

	public SimpleTokenizedText() {
		this.tokens = new String[0];
	}

	public SimpleTokenizedText(String[] tokens) {
		this.tokens = tokens;
	}

	public String[] getTokens() {
		return tokens;
	}

	public void setTokens(String[] tokens) {
		this.tokens = tokens;
	}

	@Override
	public void set(String[] values) {
		setTokens(values);
	}

	@Override
	public String[] getValueAsStrings() {
		return tokens;
	}
}
