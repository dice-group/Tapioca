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
package org.aksw.simba.tapioca.server.similarity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.aksw.simba.tapioca.server.data.SimpleVector;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CosineSimilarityTest {

    private static final double DELTA = 0.000001;

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> testConfigs = new ArrayList<Object[]>();
        testConfigs.add(new Object[] { new double[] { 1, 0 }, new double[] { 0, 1 }, 0 });
        testConfigs.add(new Object[] { new double[] { 1, 0 }, new double[] { 1, 0 }, 1 });
        testConfigs.add(new Object[] { new double[] { 1, 1 }, new double[] { 1, 0 }, 1 / Math.sqrt(2) });
        testConfigs.add(new Object[] { new double[] { 1, 1 }, new double[] { 1, 1 }, 1 });
        return testConfigs;
    }

    private SimpleVector v1;
    private SimpleVector v2;
    private double expectedSimilarity;

    public CosineSimilarityTest(double[] v1, double[] v2, double expectedSimilarity) {
        this.v1 = new SimpleVector(v1);
        this.v2 = new SimpleVector(v2);
        this.expectedSimilarity = expectedSimilarity;
    }

    @Test
    public void test() {
        CosineSimilarity sim = new CosineSimilarity();
        Assert.assertEquals(expectedSimilarity, sim.getSimilarity(v1, v2), DELTA);
    }
}
