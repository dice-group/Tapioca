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
