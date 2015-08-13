package org.aksw.simba.tapioca.extraction.voidex;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

public class PropertyDescription  extends VoidInformation {
    @Override
    public void addToCount(ObjectIntOpenHashMap<String> countedClasses,
            ObjectIntOpenHashMap<String> countedProperties) {
        if (this.isComplete()) {
            countedProperties.putOrAdd(uri, count, count);
        }
    }
}
