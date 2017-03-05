/**
 * This file is part of tapioca.indexgenerator.
 *
 * tapioca.indexgenerator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.indexgenerator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.indexgenerator.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.indexgenerator.docgen.util;

import java.util.Comparator;

import org.aksw.simba.tapioca.indexgenerator.docgen.data.StatResult;

/**
 * For comparison of two StatResult entries.
 * 
 * @author Michael Roeder
 *
 */
public class StatResultComparator implements Comparator<StatResult> {

	@Override
	public int compare(StatResult a, StatResult b) {
		if (a.timestamp != null) {
			if (b.timestamp != null) {
				if (a.timestamp.before(b.timestamp)) {
					return -1;
				} else if (b.timestamp.before(a.timestamp)) {
					return 1;
				} else {
					return Integer.compare(a.id, b.id);
				}
			} else {
				return 1;
			}
		} else {
			if (b.timestamp != null) {
				return -1;
			} else {
				return Integer.compare(a.id, b.id);
			}
		}
	}

}
