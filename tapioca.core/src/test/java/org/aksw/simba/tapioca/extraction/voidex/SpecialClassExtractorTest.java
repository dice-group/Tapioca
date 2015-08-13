package org.aksw.simba.tapioca.extraction.voidex;

import java.util.Arrays;
import java.util.Collection;

import org.aksw.simba.tapioca.extraction.AbstractExtractorTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

@RunWith(Parameterized.class)
public class SpecialClassExtractorTest extends AbstractExtractorTest {

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays
				.asList(new Object[][] {
						{
								"<http://example.org/entity1> <http://example.org/hasLabel> \"entity 1\" .\n"
										+ "<http://example.org/entity1> <" + RDF.type.getURI()
										+ "> <http://example.org/Class1> .\n" + "<http://example.org/Class2> <"
										+ RDF.type.getURI() + "> <" + RDFS.Class.getURI() + "> .\n", new String[] {},
								new int[] {} },
						{
								"<http://example.org/concept1> <"
										+ RDF.type.getURI()
										+ "> <http://www.w3.org/2004/02/skos/core#Concept> .\n"
										+ "<http://example.org/conceptScheme1> <"
										+ RDF.type.getURI()
										+ "> <http://www.w3.org/2004/02/skos/core#ConceptScheme> .\n"
										+ "<http://example.org/concept1> <http://www.w3.org/2004/02/skos/core#inScheme> <http://example.org/conceptScheme1> .",
								new String[] { "http://example.org/concept1", "http://example.org/conceptScheme1" },
								new int[] { 0, 0 } } });
	}

	private String rdfData;
	private String extractedClasses[];
	private int classesCounts[];

	public SpecialClassExtractorTest(String rdfData, String[] extractedClasses, int[] classesCounts) {
		this.rdfData = rdfData;
		this.extractedClasses = extractedClasses;
		this.classesCounts = classesCounts;
	}

	@Test
	public void test() {
		SpecialClassExtractor extractor = new SpecialClassExtractor();
		runTestOnN3(rdfData, extractor);
		checkExtractedData(extractor.getCountedSpecialClasses(), extractedClasses, classesCounts);
	}

}
