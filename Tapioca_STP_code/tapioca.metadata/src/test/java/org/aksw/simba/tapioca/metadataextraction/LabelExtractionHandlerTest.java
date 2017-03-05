/**
 * 
 */
package org.aksw.simba.tapioca.metadataextraction;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aksw.simba.tapioca.metadataextraction.LabelExtractionHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * @author Kai
 *
 */
@FixMethodOrder( MethodSorters.NAME_ASCENDING )
public class LabelExtractionHandlerTest {
	
	/**
	 * local instance of the class
	 */
	private static LabelExtractionHandler handler;
	
	/**
	 * todo before class initialization
	 * @throws Exception Throws an exception.
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// set input file
		String inFile = "src/test/data/expected_5_nolabels.nt";
		// set input model
		Model inModel = ModelFactory.createDefaultModel();
		inModel.read( inFile, "N-TRIPLES" );
		// create handler
		// create handler
		handler = new LabelExtractionHandler();
		handler.setVoidModel( inModel );

	}
	
	/**
	 * todo after all tests are done
	 * @throws Exception An Exception.
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		handler = null;
	}
	
	/**
	 * test method for LabelExtractionHandler#getRequiredUris(), expected_5.nt
	 */
	@Test
	public final void test01GetRequiredUris() {
		// set expected result
		Set<String> expected = new HashSet<String>();
		expected.add( "http://example.org/class1" );
		expected.add( "http://example.org/class2" );
		expected.add( "http://www.w3.org/2000/01/rdf-schema#Class" );
		expected.add( "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property" );
		expected.add( "http://example.org/property1" );
		expected.add( "http://example.org/property2" );
		expected.add( "http://www.w3.org/2000/01/rdf-schema#subClassOf" );
		expected.add( "http://www.w3.org/2000/01/rdf-schema#subPropertyOf" );
		expected.add( "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" );
		expected.add( "http://www.w3.org/2000/01/rdf-schema#label" );
		
		// run test
		handler.readRequiredUris();
		assertEquals( expected, handler.getUris() );
	}

	/**
	 * Test method for LabelExtractionHandler#extractLabels(), 5.nt
	 */
	@Test
	public final void test02RunExtraction() {
		// set input file
		String inFile = "src/test/data/5.nt";
		
		// set expected result
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>();
			// Class 1
			Set<String> c1 = new HashSet<String>();
			c1.add( "An example class" );
			expected.put( "http://example.org/class1", c1 );
			// Class 2
			Set<String> c2 = new HashSet<String>();
			c2.add( "Another example class" );
			expected.put( "http://example.org/class2", c2 );
			// Property 1
			Set<String> p1 = new HashSet<String>();
			p1.add( "An example property" );
			expected.put( "http://example.org/property1", p1 );
			// Property 2
			Set<String> p2 = new HashSet<String>();
			p2.add( "And a second example property" );
			expected.put( "http://example.org/property2", p2 );
			
		// run test
		try {
			handler.runExtraction( inFile );
		    assertEquals( expected, handler.getLabels() );
		}
		catch( Exception e) {
			fail( e.toString() );
		}
	}
	
	/**
	 * Test method for LabelExtractionHandler#addExtractedLabels(), 5.nt
	 */
	@Test
	public final void test03AddExtractedLabels() {
		// set input file
		String inFile = "src/test/data/expected_5.nt";

		// set expected result
		Model expected = null;
		try {
			expected = ModelFactory.createDefaultModel();
			expected.read( inFile, "N-TRIPLES" );
		}
		catch( Exception e ) {
			fail( e.toString() );
		}
		
		// run test
		handler.addExtractedLabels();
		assertTrue( expected.isIsomorphicWith( handler.getVoidModel() ) );		
	}
	
	/**
	 * Test method for LabelExtractionHandler#extractLabels(), 5.nt
	 */
	@Test
	public final void test04ExtractLabels() {
		// create new model
		handler = null;
		try {
			String inFile = "src/test/data/expected_5_nolabels.nt";
			Model inModel = ModelFactory.createDefaultModel();
			inModel.read( inFile, "N-TRIPLES" );
			handler = new LabelExtractionHandler();
			handler.setVoidModel( inModel );

		}
		catch( Exception e ) {
			fail( e.toString() );
		}
	
		// set expected result
		Model expected = null;
		try {
			String inFile = "src/test/data/expected_5.nt";
			expected = ModelFactory.createDefaultModel();
			expected.read( inFile, "N-TRIPLES" );
		}
		catch( Exception e ) {
			fail( e.toString() );
		}

		// run test
		try {
			String inFile = "src/test/data/expected_5.nt";
			assertTrue( expected.isIsomorphicWith( handler.extractLabels( inFile ) ) );
		}
		catch ( Exception e ) {
			fail( e.toString() );
		}
	}

}
