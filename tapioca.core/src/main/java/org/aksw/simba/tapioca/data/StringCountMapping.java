package org.aksw.simba.tapioca.data;

import org.aksw.simba.topicmodeling.utils.doc.AbstractSimpleDocumentProperty;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;

public class StringCountMapping extends AbstractSimpleDocumentProperty<ObjectLongOpenHashMap<String>> {

    private static final long serialVersionUID = -730869708004901960L;

    public StringCountMapping(ObjectLongOpenHashMap<String> value) {
        super(value);
    }

    public StringCountMapping() {
        this(new ObjectLongOpenHashMap<String>());
    }
}
