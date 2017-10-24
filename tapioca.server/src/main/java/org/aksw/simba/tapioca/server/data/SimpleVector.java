/**
 * tapioca.server - ${project.description}
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
package org.aksw.simba.tapioca.server.data;

public class SimpleVector {

    public double length;
    public double values[];

    public SimpleVector(double[] values) {
        this.length = getLength(values);
        this.values = values;
    }

    public SimpleVector(double[] values, double length) {
        this.length = length;
        this.values = values;
    }

    public double getLength() {
        return length;
    }

    public void setLength(double length) {
        this.length = length;
    }

    public double[] getValues() {
        return values;
    }

    public void setValues(double[] values) {
        this.values = values;
    }

    public static double getLength(double vector[]) {
        double length = 0;
        for (int i = 0; i < vector.length; ++i) {
            length += vector[i] * vector[i];
        }
        return Math.sqrt(length);
    }
}
