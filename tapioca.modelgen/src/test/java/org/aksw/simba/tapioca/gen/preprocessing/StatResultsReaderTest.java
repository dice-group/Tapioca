/**
 * tapioca.modelgen - ${project.description}
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
 * This file is part of tapioca.modelgen.
 *
 * tapioca.modelgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.modelgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.modelgen.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.gen.preprocessing;

import junit.framework.Assert;

import org.aksw.simba.tapioca.gen.data.StatResult;
import org.junit.Test;

import com.carrotsearch.hppc.IntObjectOpenHashMap;

public class StatResultsReaderTest {

    @Test
    public void testRemoveOldstatResults() {
        IntObjectOpenHashMap<StatResult> statResults = new IntObjectOpenHashMap<StatResult>();
        IntObjectOpenHashMap<StatResult> expectedStatResults = new IntObjectOpenHashMap<StatResult>();
        StatResult statResult;

        statResult = new StatResult(15925, "http://lodstats.aksw.org/stat_result/15925");
        statResult.datasetUri = "http://lodstats.aksw.org/rdfdocs/646";
        statResults.put(statResult.id, statResult);
        expectedStatResults.put(statResult.id, statResult);

        statResult = new StatResult(15888, "http://lodstats.aksw.org/stat_result/15888");
        statResult.datasetUri = "http://lodstats.aksw.org/rdfdocs/603";
        statResults.put(statResult.id, statResult);

        statResult = new StatResult(19061, "http://lodstats.aksw.org/stat_result/19061");
        statResult.datasetUri = "http://lodstats.aksw.org/rdfdocs/603";
        statResults.put(statResult.id, statResult);

        statResult = new StatResult(21124, "http://lodstats.aksw.org/stat_result/21124");
        statResult.datasetUri = "http://lodstats.aksw.org/rdfdocs/603";
        statResults.put(statResult.id, statResult);
        expectedStatResults.put(statResult.id, statResult);

        StatResultsReader reader = new StatResultsReader();
        statResults = reader.removeOldstatResults(statResults);

        Assert.assertEquals(expectedStatResults.assigned, statResults.assigned);
        for (int i = 0; i < expectedStatResults.allocated.length; ++i) {
            if (expectedStatResults.allocated[i]) {
                Assert.assertTrue(statResults.containsKey(expectedStatResults.keys[i]));
                Assert.assertEquals(((Object[]) expectedStatResults.values)[i],
                        statResults.get(expectedStatResults.keys[i]));
            }
        }
    }
}
