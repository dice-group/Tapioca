package org.aksw.simba.tapioca.extraction.voidex;

import java.util.Arrays;
import java.util.Collection;

import org.aksw.simba.tapioca.extraction.AbstractExtractorTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

@RunWith(Parameterized.class)
public class VoidExtractorTest extends AbstractExtractorTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays
                .asList(new Object[][] {
                        // test "normal" VoID extraction and the extraction of a
                        // class that is only defined but has no instances
                        { "<http://example.org/entity1> <http://example.org/hasLabel> \"entity 1\" .\n"
                                + "<http://example.org/entity1> <" + RDF.type.getURI()
                                + "> <http://example.org/Class1> .\n"
                                + "<http://example.org/Class2> <" + RDF.type.getURI() + "> <" + RDFS.Class
                                        .getURI()
                                + "> .\n",
                                new String[] { "http://example.org/Class1", "http://example.org/Class2",
                                        RDFS.Class.getURI() },
                                new int[] { 1, 0, 1 },
                                new String[] { RDF.type.getURI(), "http://example.org/hasLabel" }, new int[] { 2, 1 } },
                        // test the definition of properties
                        { "<http://example.org/property1> <" + RDF.type.getURI() + "> <" + OWL.ObjectProperty.getURI()
                                + "> .\n" + "<http://example.org/property1> <" + RDF.type.getURI() + "> <"
                                + OWL.TransitiveProperty.getURI() + "> .\n"
                                + "<http://example.org/entity1> <http://example.org/property1> <http://example.org/entity2> .\n",
                                new String[] { OWL.ObjectProperty.getURI(), OWL.TransitiveProperty.getURI() },
                                new int[] { 1, 1 }, new String[] { RDF.type.getURI(), "http://example.org/property1" },
                                new int[] { 2, 1 } },
                        // Make sure that blank nodes are not extracted
                        { "<http://example.org/property1> <" + RDF.type.getURI() + "> _:b .\n" + "_:b <"
                                + RDF.type.getURI() + "> <" + RDFS.Class.getURI() + "> .\n" + "_:p <"
                                + RDF.type.getURI() + "> <" + RDF.Property.getURI() + "> .\n",
                                new String[] { RDFS.Class.getURI(), RDF.Property.getURI() }, new int[] { 1, 1 },
                                new String[] { RDF.type.getURI() }, new int[] { 3 } },
                        // test subClassOf and subPropertyOf properties
                        { "<http://example.org/class1> <" + RDFS.subClassOf.getURI()
                                + "> <http://example.org/class2> .\n" + "<http://example.org/property1> <"
                                + RDFS.subPropertyOf.getURI() + "> <http://example.org/property2> .\n",
                                new String[] { "http://example.org/class1", "http://example.org/class2" },
                                new int[] { 0, 0 },
                                new String[] { "http://example.org/property1", "http://example.org/property2",
                                        RDFS.subClassOf.getURI(), RDFS.subPropertyOf.getURI() },
                        new int[] { 0, 0, 1, 1 } } });
    }

    private String rdfData;
    private String extractedClasses[];
    private int classesCounts[];
    private String extractedProperties[];
    private int propertyCounts[];

    public VoidExtractorTest(String rdfData, String[] extractedClasses, int[] classesCounts,
            String[] extractedProperties, int[] propertyCounts) {
        this.rdfData = rdfData;
        this.extractedClasses = extractedClasses;
        this.classesCounts = classesCounts;
        this.extractedProperties = extractedProperties;
        this.propertyCounts = propertyCounts;
    }

    @Test
    public void test() {
        VoidExtractor extractor = new VoidExtractor();
        runTestOnN3(rdfData, extractor);
        checkExtractedData(extractor.getCountedClasses(), extractedClasses, classesCounts);
        checkExtractedData(extractor.getCountedProperties(), extractedProperties, propertyCounts);
    }

}
