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
