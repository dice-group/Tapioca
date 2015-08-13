package org.aksw.simba.tapioca.extraction.voidex;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

public abstract class VoidInformation {
    public String uri;
    public int count;

    public boolean isComplete() {
        return uri != null;
    }

    public abstract void addToCount(ObjectIntOpenHashMap<String> countedClasses,
            ObjectIntOpenHashMap<String> countedProperties);
}