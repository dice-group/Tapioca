/**
 * This file is part of tapioca.core.
 *
 * tapioca.core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.core.  If not, see <http://www.gnu.org/licenses/>.
 */
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
