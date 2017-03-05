/**
 * This file is part of tapioca.server.
 *
 * tapioca.server is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.server is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.server.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.server.similarity;

import org.aksw.simba.tapioca.server.data.SimpleVector;

public class CosineSimilarity implements VectorSimilarity {

    @Override
    public double getSimilarity(SimpleVector v1, SimpleVector v2) {
        if (v1.values.length != v2.values.length) {
            throw new IllegalArgumentException("Got two vectors with different lengths.");
        }
        double sim = 0;
        for (int i = 0; i < v1.values.length; ++i) {
            sim += v1.values[i] * v2.values[i];
        }
        sim /= v1.length * v2.length;
        return sim;
    }

}
