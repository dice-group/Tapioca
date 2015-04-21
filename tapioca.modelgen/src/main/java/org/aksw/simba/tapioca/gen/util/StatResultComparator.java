package org.aksw.simba.tapioca.gen.util;

import java.util.Comparator;

import org.aksw.simba.tapioca.gen.data.StatResult;

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
