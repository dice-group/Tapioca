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
package org.aksw.simba.tapioca.cores.extraction;

import org.apache.jena.riot.lang.PipedRDFIterator;

import com.hp.hpl.jena.graph.Triple;

public abstract class AbstractExtractor implements Extractor {

	public void extract(PipedRDFIterator<Triple> iter) {
		while (iter.hasNext()) {
			handleTriple(iter.next());
		}
	}
}
