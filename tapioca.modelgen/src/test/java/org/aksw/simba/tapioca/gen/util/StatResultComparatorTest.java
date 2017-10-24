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
package org.aksw.simba.tapioca.gen.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.aksw.simba.tapioca.gen.data.StatResult;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class StatResultComparatorTest {

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> testConfigs = new ArrayList<Object[]>();
        StatResult a, b;

        a = new StatResult(1, "http://lodstats.aksw.org/stat_result/1");
        a.timestamp = new Date(1);
        b = new StatResult(2, "http://lodstats.aksw.org/stat_result/2");
        b.timestamp = new Date(2);
        testConfigs.add(new Object[] { a, b, -1 });

        a = new StatResult(1, "http://lodstats.aksw.org/stat_result/1");
        a.timestamp = new Date(3);
        b = new StatResult(2, "http://lodstats.aksw.org/stat_result/2");
        b.timestamp = new Date(2);
        testConfigs.add(new Object[] { a, b, 1 });

        a = new StatResult(1, "http://lodstats.aksw.org/stat_result/1");
        a.timestamp = new Date(1);
        b = new StatResult(2, "http://lodstats.aksw.org/stat_result/2");
        b.timestamp = new Date(1);
        testConfigs.add(new Object[] { a, b, -1 });

        a = new StatResult(1, "http://lodstats.aksw.org/stat_result/1");
        a.timestamp = new Date(1);
        b = new StatResult(1, "http://lodstats.aksw.org/stat_result/1");
        b.timestamp = new Date(1);
        testConfigs.add(new Object[] { a, b, 0 });

        a = new StatResult(1, "http://lodstats.aksw.org/stat_result/1");
        a.timestamp = new Date(1);
        b = new StatResult(2, "http://lodstats.aksw.org/stat_result/2");
        testConfigs.add(new Object[] { a, b, 1 });

        a = new StatResult(1, "http://lodstats.aksw.org/stat_result/1");
        b = new StatResult(2, "http://lodstats.aksw.org/stat_result/2");
        testConfigs.add(new Object[] { a, b, -1 });

        a = new StatResult(1, "http://lodstats.aksw.org/stat_result/1");
        b = new StatResult(1, "http://lodstats.aksw.org/stat_result/1");
        testConfigs.add(new Object[] { a, b, 0 });

        return testConfigs;
    }

    private StatResult a;
    private StatResult b;
    private int expectedResult;

    public StatResultComparatorTest(StatResult a, StatResult b, int expectedResult) {
        this.a = a;
        this.b = b;
        this.expectedResult = expectedResult;
    }

    @Test
    public void test() {
        StatResultComparator comparator = new StatResultComparator();
        Assert.assertEquals(expectedResult, comparator.compare(a, b));
        Assert.assertEquals(expectedResult * -1, comparator.compare(b, a));
    }
}
