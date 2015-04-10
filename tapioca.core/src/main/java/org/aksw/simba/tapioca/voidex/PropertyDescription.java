package org.aksw.simba.tapioca.voidex;

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
