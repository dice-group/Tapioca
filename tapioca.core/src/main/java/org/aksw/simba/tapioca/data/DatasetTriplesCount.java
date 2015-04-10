package org.aksw.simba.tapioca.data;

import org.aksw.simba.topicmodeling.utils.doc.AbstractDocumentProperty;

public class DatasetTriplesCount extends AbstractDocumentProperty {

    private static final long serialVersionUID = -1L;
    
    private long triples;

    public DatasetTriplesCount(long triples) {
	this.triples = triples;
    }

    @Override
    public Object getValue() {
	return triples;
    }

    /**
     * @return the triples
     */
    public long getTriples() {
	return triples;
    }

    /**
     * @param triples
     *            the triples to set
     */
    public void setTriples(long triples) {
	this.triples = triples;
    }

    @Override
    public int hashCode() {
	return (int) (triples ^ (triples >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (!super.equals(obj))
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	DatasetTriplesCount other = (DatasetTriplesCount) obj;
	if (triples != other.triples)
	    return false;
	return true;
    }

}
