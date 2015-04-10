package org.aksw.simba.tapioca.data;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;

public class DatasetPropertyInfo extends DatasetLODStatsInfo {
    
    private static final long serialVersionUID = 1L;

    public DatasetPropertyInfo() {
    }

    public DatasetPropertyInfo(ObjectLongOpenHashMap<String> countedURIs) {
        super(countedURIs);
    }
}
