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
package org.aksw.simba.tapioca.cores.data;

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
