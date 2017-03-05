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

import java.util.List;

/**
 * Interface for returning labels
 * 
 * @author Michael Roeder
 *
 */
public interface TokenizedLabelRetriever {
	
	/**
	 * Return labels to URI
	 * 
	 * @param uri specific URI for which labels are needed
	 * @param namespace Namespace of the URI
	 * @return List of labels connected to the URI
	 */
	public List<String> getTokenizedLabel(String uri, String namespace);
}
