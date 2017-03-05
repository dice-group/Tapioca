/**
 * 
 */
package org.aksw.simba.tapioca.metadataextraction;

import static org.junit.Assert.*;

import org.aksw.simba.tapioca.metadataextraction.VoidExtractionHandler;
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
public class VoidExtractionHandlerTest {

	/**
	 * Test method for VoidExtractionHandler#extractVoidInfo().
	 */
	@Test
	public final void test00ExtractVoidInfo() {
		// set input file
		String inFile = "src/test/data/0.nt";
		String exFile = "src/test/data/expected_0.nt";
		// create handler
		VoidExtractionHandler handler = new VoidExtractionHandler();
		
		// run test
		try {
			// expected result
			Model expected = ModelFactory.createDefaultModel();
			expected.read( exFile, "N-TRIPLES" );
			// extract 
			Model voidModel = handler.extractVoidInfo( "http://example.org/testdata#0.nt" , inFile );
			// print
			System.out.println( "\nEXPECTED:" );
			expected.write( System.out , "N-TRIPLES" );
			System.out.println( "\nRESULT:" );
			voidModel.write( System.out , "N-TRIPLES" );
			// test
			assertTrue( expected.isIsomorphicWith( voidModel ) );
		}
		catch( Exception e ) {
			fail( e.toString() );
		}
	}

	/**
	 * Test method for VoidExtractionHandler#extractVoidInfo().
	 */
	@Test
	public final void test03ExtractVoidInfo() {
		// set input file
		String inFile = "src/test/data/3.nt";
		String exFile = "src/test/data/expected_3.nt";
		// create handler
		VoidExtractionHandler handler = new VoidExtractionHandler();
		
		// run test
		try {
			// expected result
			Model expected = ModelFactory.createDefaultModel();
			expected.read( exFile, "N-TRIPLES" );
			// extract 
			Model voidModel = handler.extractVoidInfo( "http://example.org/testdata#3.nt" , inFile );
			// print
			System.out.println( "\nEXPECTED:" );
			expected.write( System.out , "N-TRIPLES" );
			System.out.println( "\nRESULT:" );
			voidModel.write( System.out , "N-TRIPLES" );
			// test
			assertTrue( expected.isIsomorphicWith( voidModel ) );
		}
		catch( Exception e ) {
			fail( e.toString() );
		}
	}

	/**
	 * Test method for VoidExtractionHandler#extractVoidInfo().
	 */
	@Test
	public final void test05ExtractVoidInfo() {
		// set input file
		String inFile = "src/test/data/5.nt";
		String exFile = "src/test/data/expected_5_nolabels.nt";
		// create handler
		VoidExtractionHandler handler = new VoidExtractionHandler();
		
		// run test
		try {
			// expected result
			Model expected = ModelFactory.createDefaultModel();
			expected.read( exFile, "N-TRIPLES" );
			// extract 
			Model voidModel = handler.extractVoidInfo( "http://example.org/testdata#5.nt" , inFile );
			// print
			System.out.println( "\nEXPECTED:" );
			expected.write( System.out , "N-TRIPLES" );
			System.out.println( "\nRESULT:" );
			voidModel.write( System.out , "N-TRIPLES" );
			// test
			assertTrue( expected.isIsomorphicWith( voidModel ) );
		}
		catch( Exception e ) {
			fail( e.toString() );
		}
	}

	
}
