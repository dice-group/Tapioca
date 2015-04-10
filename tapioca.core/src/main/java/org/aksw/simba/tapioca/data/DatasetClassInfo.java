package org.aksw.simba.tapioca.data;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;

public class DatasetClassInfo extends DatasetLODStatsInfo {
    
    private static final long serialVersionUID = 1L;

    public DatasetClassInfo() {
    }

    public DatasetClassInfo(ObjectLongOpenHashMap<String> countedURIs) {
        super(countedURIs);
    }
}
