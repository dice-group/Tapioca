package org.aksw.simba.tapioca.preprocessing;

import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyEditingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.AbstractSimpleDocumentProperty;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import com.carrotsearch.hppc.ObjectOpenHashSet;

public class SimpleBlankNodeRemovingDocumentSupplierDecorator<T extends AbstractSimpleDocumentProperty<ObjectLongOpenHashMap<String>>>
        extends AbstractPropertyEditingDocumentSupplierDecorator<T> {

    public SimpleBlankNodeRemovingDocumentSupplierDecorator(DocumentSupplier documentSource, Class<T> propertyClass) {
        super(documentSource, propertyClass);
    }

    @Override
    protected void editDocumentProperty(T property) {
        ObjectLongOpenHashMap<String> map = property.get();
        ObjectOpenHashSet<String> removableURIs = new ObjectOpenHashSet<String>();
        String uri;
        for (int i = 0; i < map.allocated.length; ++i) {
            if (map.allocated[i]) {
                uri = (String) ((Object[]) map.keys)[i];
                // If this URI seems to be a blank node
                if (uri.startsWith("_:node") || uri.startsWith("node")) {
                    removableURIs.add(uri);
                }
            }
        }
        map.removeAll(removableURIs);
    }

}
