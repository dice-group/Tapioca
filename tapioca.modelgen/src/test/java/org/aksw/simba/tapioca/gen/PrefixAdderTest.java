package org.aksw.simba.tapioca.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PrefixAdderTest {

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> testConfigs = new ArrayList<Object[]>();

        Model model;

        model = ModelFactory.createDefaultModel();
        testConfigs.add(new Object[] { model, new String[] {} });

        model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/a"), RDF.type, RDFS.Class);
        testConfigs.add(new Object[] { model, new String[] {} });

        model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/a"), RDF.type, RDFS.Class);
        model.add(model.getResource("http://example.org/b"), RDF.type, RDFS.Class);
        model.add(model.getResource("http://example.org/c"), RDF.type, RDFS.Class);
        testConfigs.add(new Object[] { model, new String[] { "http://example.org/", RDF.getURI(), RDFS.getURI() } });

        model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/a"), model.getProperty("http://example.org/p"), "1",
                XSDDatatype.XSDinteger);
        model.add(model.getResource("http://example.org/a"), model.getProperty("http://example.org/p"), "1",
                XSDDatatype.XSDint);
        model.add(model.getResource("http://example.org/a"), model.getProperty("http://example.org/p"), "1",
                XSDDatatype.XSDlong);
        testConfigs.add(new Object[] { model, new String[] { "http://example.org/", XSD.getURI() } });

        return testConfigs;
    }

    protected Model model;
    protected Set<String> expectedNamespaces;

    public PrefixAdderTest(Model model, String[] expectedNamespaces) {
        this.model = model;
        this.expectedNamespaces = new HashSet<String>(Arrays.asList(expectedNamespaces));
    }

    @Test
    public void test() {
        PrefixAdder.addPrefixes(model);
        Map<String, String> prefixes = model.getNsPrefixMap();
        for (Entry<String, String> e : prefixes.entrySet()) {
            Assert.assertTrue(e.getValue() + " was not expected", expectedNamespaces.contains(e.getValue()));
        }
        Assert.assertEquals("Not all expected URIs could be found. Expected: " + expectedNamespaces.toString()
                + " Actual: " + prefixes.toString(), expectedNamespaces.size(), prefixes.size());
    }
}
