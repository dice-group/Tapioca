package org.aksw.simba.tapioca.data;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;

public class DatasetSpecialClassesInfo extends DatasetLODStatsInfo {
    
    private static final long serialVersionUID = 1L;

    public DatasetSpecialClassesInfo() {
    }

    public DatasetSpecialClassesInfo(ObjectLongOpenHashMap<String> countedURIs) {
        super(countedURIs);
    }
}
