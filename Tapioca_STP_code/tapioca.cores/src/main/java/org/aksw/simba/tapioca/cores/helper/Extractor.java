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
package org.aksw.simba.tapioca.cores.helper;

import org.apache.jena.riot.lang.PipedRDFIterator;
import com.hp.hpl.jena.graph.Triple;

/**
 * Define Extractor interface
 * 
 * @author Michael Roeder
 *
 */
public interface Extractor {
	
	/**
	 * Method to extract metadata from a RDF model, split
	 * into triples, piped by a stream 
	 * @param iter Iterator, holding the RDF triples
	 */
	public void extract(PipedRDFIterator<Triple> iter);
	
	/**
	 * Method to extract metadata from a RDF triple
	 * @param triple RDF triple
	 */
	public void handleTriple(Triple triple);

}
