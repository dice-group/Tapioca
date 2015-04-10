package org.aksw.simba.tapioca.voidex;

import java.util.HashSet;
import java.util.Set;

import org.aksw.simba.tapioca.data.vocabularies.VOID;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class VoidExtractingRdfHandler extends RDFHandlerBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(VoidExtractingRdfHandler.class);

    private static final String RDF_TYPE_URI = RDF.type.getURI();
    private static final String RDFS_CLASS_URI = RDFS.Class.getURI();
    private static final String OWL_CLASS_URI = OWL.Class.getURI();
    private static final String RDF_PROPERTY_URI = RDF.Property.getURI();

    @Deprecated
    private static final Set<String> specialClasses = new HashSet<String>();// VoidExtractor.loadSpecialClassesList();

    private ObjectIntOpenHashMap<String> countedClasses;
    private ObjectIntOpenHashMap<String> countedProperties;
    private ObjectIntOpenHashMap<String> countedSpecialClasses;
    private ObjectObjectOpenHashMap<String, VoidInformation> voidInformation;

    public VoidExtractingRdfHandler() {
        countedClasses = new ObjectIntOpenHashMap<String>();
        countedProperties = new ObjectIntOpenHashMap<String>();
    }

    public VoidExtractingRdfHandler(ObjectIntOpenHashMap<String> countedClasses,
            ObjectIntOpenHashMap<String> countedProperties, ObjectIntOpenHashMap<String> countedSpecialClasses,
            ObjectObjectOpenHashMap<String, VoidInformation> voidInformation) {
        this.countedClasses = countedClasses;
        this.countedProperties = countedProperties;
        this.countedSpecialClasses = countedSpecialClasses;
        this.voidInformation = voidInformation;
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        String predicate = st.getPredicate().stringValue();
        if (predicate.equals(RDF_TYPE_URI) && !(st.getObject() instanceof BNode)) {
            String objectURI = st.getObject().stringValue();
            if (RDFS_CLASS_URI.equals(objectURI) | OWL_CLASS_URI.equals(objectURI)) {
                countedClasses.putOrAdd(st.getSubject().stringValue(), 0, 0);
            } else if (RDF_PROPERTY_URI.equals(objectURI)) {
                countedProperties.putOrAdd(predicate, 0, 0);
            } else if (specialClasses.contains(objectURI)) {
                if (!(st.getSubject() instanceof BNode)) {
                    countedSpecialClasses.putOrAdd(st.getSubject().stringValue(), 0, 0);
                }
            }
            countedClasses.putOrAdd(objectURI, 1, 1);
        } else if (predicate.startsWith(VOID.getURI())) {
            // This predicate is part of the VOID vocabulary
            String subjectURI = st.getSubject().stringValue();
            if (predicate.equals(VOID.clazz)) {
                ClassDescription classDesc;
                if (voidInformation.containsKey(subjectURI)) {
                    classDesc = (ClassDescription) voidInformation.get(subjectURI);
                } else {
                    classDesc = new ClassDescription();
                    voidInformation.put(subjectURI, classDesc);
                }
                classDesc.uri = st.getObject().stringValue();
            } else if (predicate.equals(VOID.entities)) {
                ClassDescription classDesc;
                if (voidInformation.containsKey(subjectURI)) {
                    classDesc = (ClassDescription) voidInformation.get(subjectURI);
                } else {
                    classDesc = new ClassDescription();
                    voidInformation.put(subjectURI, classDesc);
                }
                try {
                    classDesc.count = ((Literal) st.getObject()).intValue();
                } catch (Exception e) {
                    LOGGER.error("Tried to parse the entities count from \"" + st.toString() + "\".", e);
                }
            } else if (predicate.equals(VOID.property)) {
                PropertyDescription propertyDesc;
                if (voidInformation.containsKey(subjectURI)) {
                    propertyDesc = (PropertyDescription) voidInformation.get(subjectURI);
                } else {
                    propertyDesc = new PropertyDescription();
                    voidInformation.put(subjectURI, propertyDesc);
                }
                propertyDesc.uri = st.getObject().stringValue();
            } else if (predicate.equals(VOID.triples)) {
                PropertyDescription propertyDesc;
                if (voidInformation.containsKey(subjectURI)) {
                    propertyDesc = (PropertyDescription) voidInformation.get(subjectURI);
                } else {
                    propertyDesc = new PropertyDescription();
                    voidInformation.put(subjectURI, propertyDesc);
                }
                try {
                    propertyDesc.count = ((Literal) st.getObject()).intValue();
                } catch (Exception e) {
                    LOGGER.error("Tried to parse the entities count from \"" + st.toString() + "\".", e);
                }
            }
            super.handleStatement(st);
        }

        // Count the property ( it doesn't matter whether it is a void property
        // or not)
        countedProperties.putOrAdd(predicate, 1, 1);
        super.handleStatement(st);
    }

    public ObjectIntOpenHashMap<String> getCountedClasses() {
        return countedClasses;
    }

    public void setCountedClasses(ObjectIntOpenHashMap<String> countedClasses) {
        this.countedClasses = countedClasses;
    }

    public ObjectIntOpenHashMap<String> getCountedProperties() {
        return countedProperties;
    }

    public void setCountedProperties(ObjectIntOpenHashMap<String> countedProperties) {
        this.countedProperties = countedProperties;
    }

    public ObjectObjectOpenHashMap<String, VoidInformation> getVoidInformation() {
        return voidInformation;
    }

    public void setVoidInformation(ObjectObjectOpenHashMap<String, VoidInformation> voidInformation) {
        this.voidInformation = voidInformation;
    }

    @Deprecated
    public void merge(VoidExtractingRdfHandler otherHandler) {
        mergeCounts(this.countedClasses, otherHandler.countedClasses);
        mergeCounts(this.countedProperties, otherHandler.countedProperties);
    }

    @Deprecated
    @SuppressWarnings("unchecked")
    protected static <T> void mergeCounts(ObjectIntOpenHashMap<T> counts1, ObjectIntOpenHashMap<T> counts2) {
        for (int i = 0; i < counts2.allocated.length; ++i) {
            if (counts2.allocated[i]) {
                counts1.putOrAdd(((T) ((Object[]) counts2.keys)[i]), counts2.values[i], counts2.values[i]);
            }
        }
    }
}
