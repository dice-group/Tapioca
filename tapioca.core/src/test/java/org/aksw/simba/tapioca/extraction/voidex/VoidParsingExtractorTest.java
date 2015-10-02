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

import org.aksw.simba.tapioca.data.vocabularies.VOID;
import org.aksw.simba.tapioca.extraction.AbstractExtractorTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;

@RunWith(Parameterized.class)
public class VoidParsingExtractorTest extends AbstractExtractorTest {

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays
				.asList(new Object[][] {
						{
								"<http://example.org/Dataset1> a <" + VOID.Dataset.getURI() + ">;\n<"
										+ VOID.classPartition.getURI() + "> [\n<" + VOID.clazz.getURI()
										+ "> <http://xmlns.com/foaf/0.1/Person>;\n<" + VOID.entities.getURI()
										+ "> 312000;\n];\n<" + VOID.propertyPartition.getURI() + "> [\n<"
										+ VOID.property.getURI() + "> <http://xmlns.com/foaf/0.1/name>;\n<"
										+ VOID.triples + "> 312000;\n];\n.\n",
								new String[] { "http://xmlns.com/foaf/0.1/Person" }, new int[] { 312000 },
								new String[] { "http://xmlns.com/foaf/0.1/name" }, new int[] { 312000 } },
						{
								"<http://example.org/Dataset1> a <" + VOID.Dataset.getURI() + ">;\n<"
										+ VOID.classPartition.getURI() + "> [\n<" + VOID.clazz.getURI()
										+ "> <http://xmlns.com/foaf/0.1/Person>;\n<" + VOID.entities.getURI()
										+ "> 12;\n];\n<" + VOID.propertyPartition.getURI() + "> [\n<"
										+ VOID.property.getURI() + "> <http://xmlns.com/foaf/0.1/name>;\n<"
										+ VOID.triples + "> 33;\n];\n<" + VOID.classPartition.getURI() + "> [\n<"
										+ VOID.clazz.getURI() + "> <http://xmlns.com/foaf/0.1/Person2>;\n];\n.\n",
								new String[] { "http://xmlns.com/foaf/0.1/Person", "http://xmlns.com/foaf/0.1/Person2" },
								new int[] { 12, 0 }, new String[] { "http://xmlns.com/foaf/0.1/name" },
								new int[] { 33 } } });
	}

	private String rdfData;
	private String extractedClasses[];
	private int classesCounts[];
	private String extractedProperties[];
	private int propertyCounts[];

	public VoidParsingExtractorTest(String rdfData, String[] extractedClasses, int[] classesCounts,
			String[] extractedProperties, int[] propertyCounts) {
		this.rdfData = rdfData;
		this.extractedClasses = extractedClasses;
		this.classesCounts = classesCounts;
		this.extractedProperties = extractedProperties;
		this.propertyCounts = propertyCounts;
	}

	@Test
	public void test() {
		VoidParsingExtractor extractor = new VoidParsingExtractor();
		runTestOnTTL(rdfData, extractor);

		ObjectObjectOpenHashMap<String, VoidInformation> voidInformation = extractor.getVoidInformation();
		ObjectIntOpenHashMap<String> countedClasses = new ObjectIntOpenHashMap<String>();
		ObjectIntOpenHashMap<String> countedProperties = new ObjectIntOpenHashMap<String>();
		for (int i = 0; i < voidInformation.allocated.length; ++i) {
			if (voidInformation.allocated[i]) {
				((VoidInformation) ((Object[]) voidInformation.values)[i])
						.addToCount(countedClasses, countedProperties);
			}
		}

		checkExtractedData(countedClasses, extractedClasses, classesCounts);
		checkExtractedData(countedProperties, extractedProperties, propertyCounts);
	}

}
