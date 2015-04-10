package org.aksw.simba.tapioca.voidex;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

public class ClassDescription extends VoidInformation {
    @Override
    public void addToCount(ObjectIntOpenHashMap<String> countedClasses,
            ObjectIntOpenHashMap<String> countedProperties) {
        if (this.isComplete()) {
            countedClasses.putOrAdd(uri, count, count);
        }
    }
}
