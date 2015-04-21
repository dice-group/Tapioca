package org.aksw.simba.tapioca.gen.data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.aksw.simba.topicmodeling.utils.doc.AbstractSimpleDocumentProperty;

import com.carrotsearch.hppc.ObjectOpenHashSet;

public class DatasetURIs extends AbstractSimpleDocumentProperty<ObjectOpenHashSet<String>> implements Externalizable {

    public DatasetURIs() {
        super(new ObjectOpenHashSet<String>());
    }

    public DatasetURIs(ObjectOpenHashSet<String> value) {
        super(value);
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        get().add((String[]) oi.readObject());
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        oo.writeObject(get().toArray(String.class));
    }

}
