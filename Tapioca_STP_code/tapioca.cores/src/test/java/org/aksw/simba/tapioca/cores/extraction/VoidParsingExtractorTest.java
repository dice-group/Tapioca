/**
 * This package counts classes and properties within a 
 * RDF data set. 
 */
package org.aksw.simba.tapioca.cores.extraction;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.InputStream;

import org.aksw.simba.tapioca.cores.extraction.VoidParsingExtractor;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.hp.hpl.jena.graph.Triple;

/**
 * @author Kai
 *
 */
@FixMethodOrder( MethodSorters.NAME_ASCENDING )
public class VoidParsingExtractorTest {

	/**
	 * Test method for VoidParsingExtractor#getVoidInformation()
	 * 0.nt
	 */
	@Test
	public final void test00GetVoidInformation() {
		// set input file
		String inFile = "src/test/data/0.nt";
		// run extraction
		try {
			// setup
			InputStream inStream = new FileInputStream( inFile );
			PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
			PipedRDFStream<Triple> rdfStream = new PipedTriplesStream( iter );
			// run
			RDFDataMgr.parse(rdfStream,	inStream, "", RDFLanguages.resourceNameToLang( inFile ) );
			VoidParsingExtractor extractor = new VoidParsingExtractor();
			extractor.extract( iter );
			// print
			System.out.println( "0.nt\n  " + extractor.getVoidInformation().toString() );
			// test
			assertEquals( 0, extractor.getVoidInformation().size() );
		}
		catch( Exception e ) {
			fail( e.toString() );
		}
	}
	
	/**
	 * Test method for VoidParsingExtractor#getVoidInformation()
	 * 5.nt
	 */
	@Test
	public final void test051GetVoidInformation() {
		// set input file
		String inFile = "src/test/data/5.nt";
		// run extraction
		try {
			// setup
			InputStream inStream = new FileInputStream( inFile );
			PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
			PipedRDFStream<Triple> rdfStream = new PipedTriplesStream( iter );
			// run
			RDFDataMgr.parse(rdfStream,	inStream, "", RDFLanguages.resourceNameToLang( inFile ) );
			VoidParsingExtractor extractor = new VoidParsingExtractor();
			extractor.extract( iter );
			// print
			System.out.println( "5.nt:\n  " + extractor.getVoidInformation().toString() );
			// test
			assertEquals( 0, extractor.getVoidInformation().size() );
		}
		catch( Exception e ) {
			fail( e.toString() );
		}
	}
		
	/**
	 * Test method for VoidParsingExtractor#getVoidInformation()
	 * expected_5.nt
	 */
	@Test
	public final void test052GetVoidInformation() {
		// set input file
		String inFile = "src/test/data/expected_5.nt";
		// run extraction
		try {
			// setup
			InputStream inStream = new FileInputStream( inFile );
			PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
			PipedRDFStream<Triple> rdfStream = new PipedTriplesStream( iter );
			// run
			RDFDataMgr.parse(rdfStream,	inStream, "", RDFLanguages.resourceNameToLang( inFile ) );
			VoidParsingExtractor extractor = new VoidParsingExtractor();
			extractor.extract( iter );
			// print
			System.out.println( "expected_5.nt:\n  " + extractor.getVoidInformation().toString() );
			// test
			assertEquals( 11, extractor.getVoidInformation().size() );
		}
		catch( Exception e ) {
			fail( e.toString() );
		}		
	}

}
