/**
 * This package counts classes and properties within a 
 * RDF data set.
 */
package org.aksw.simba.tapioca.cores.extraction;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.InputStream;

import org.aksw.simba.tapioca.cores.extraction.VoidExtractor;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.hp.hpl.jena.graph.Triple;

/**
 * Test of "VoidExtractor"
 * @author Kai
 *
 */
@FixMethodOrder( MethodSorters.NAME_ASCENDING )
public class VoidExtractorTest {

	/**
	 * Test method for VoidExtractor#getCountedClasses().
	 * 0.nt
	 */
	@Test
	public final void test00GetCountedClasses() {
		// set input file
		String inFile = "src/test/data/0.nt";
		// set expected result
		ObjectIntOpenHashMap<String> expected = new ObjectIntOpenHashMap<String>();
		expected.putOrAdd( "http://example.org/Class1", 1, 1 );
		expected.putOrAdd( "http://example.org/Class2", 0, 0 );
		expected.putOrAdd( "http://www.w3.org/2000/01/rdf-schema#Class", 1, 1 );
		// run extraction
		try {
			// setup
			InputStream inStream = new FileInputStream( inFile );
			PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
			PipedRDFStream<Triple> rdfStream = new PipedTriplesStream( iter );
			// run
			RDFDataMgr.parse(rdfStream,	inStream, "", RDFLanguages.resourceNameToLang( inFile ) );
			VoidExtractor extractor = new VoidExtractor();
			extractor.extract( iter );
			// print
			System.out.println( "0.nt:\n  CLASSES: " + extractor.getCountedClasses().toString() );
			// test
			assertEquals( expected, extractor.getCountedClasses() );
		}
		catch( Exception e ) {
			fail( e.toString() );
		}
	}
	
	/**
	 * Test method for VoidExtractor#getCountedClasses().
	 * 5.nt
	 */
	@Test
	public final void test05GetCountedClasses() {
		// set input file
		String inFile = "src/test/data/5.nt";
		// set expected result
		ObjectIntOpenHashMap<String> expected = new ObjectIntOpenHashMap<String>();
		expected.putOrAdd( "http://example.org/class2", 0, 0 );
		expected.putOrAdd( "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property", 2, 2 );
		expected.putOrAdd( "http://example.org/class1", 0, 0 );
		expected.putOrAdd( "http://www.w3.org/2000/01/rdf-schema#Class", 2, 2 );
		// run extraction
		try {
			// setup
			InputStream inStream = new FileInputStream( inFile );
			PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
			PipedRDFStream<Triple> rdfStream = new PipedTriplesStream( iter );
			// run
			RDFDataMgr.parse(rdfStream,	inStream, "", RDFLanguages.resourceNameToLang( inFile ) );
			VoidExtractor extractor = new VoidExtractor();
			extractor.extract( iter );
			// print
			System.out.println( "5.nt:\n  CLASSES: " + extractor.getCountedClasses().toString() );
			// test
			assertEquals( expected, extractor.getCountedClasses() );
		}
		catch( Exception e ) {
			fail( e.toString() );
		}
	}

	/**
	 * Test method for VoidExtractor#getCountedProperties().
	 * 0.nt
	 */
	@Test
	public final void test10GetCountedProperties() {
		// set input file
		String inFile = "src/test/data/0.nt";
		// set expected result
		ObjectIntOpenHashMap<String> expected = new ObjectIntOpenHashMap<String>();
		expected.putOrAdd( "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", 2, 2 );
		expected.putOrAdd( "http://www.w3.org/2000/01/rdf-schema#label", 1, 1 );
		// run extraction
		try {
			// setup
			InputStream inStream = new FileInputStream( inFile );
			PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
			PipedRDFStream<Triple> rdfStream = new PipedTriplesStream( iter );
			// run
			RDFDataMgr.parse(rdfStream,	inStream, "", RDFLanguages.resourceNameToLang( inFile ) );
			VoidExtractor extractor = new VoidExtractor();
			extractor.extract( iter );
			// print
			System.out.println( "0.nt:\n  PROPERTIES: " + extractor.getCountedProperties().toString() );
			// test
			assertEquals( expected, extractor.getCountedProperties() );
		}
		catch( Exception e ) {
			fail( e.toString() );
		}
	}
	
	/**
	 * Test method for VoidExtractor#getCountedProperties().
	 * 0.nt
	 */
	@Test
	public final void test15GetCountedProperties() {
		// set input file
		String inFile = "src/test/data/5.nt";
		// set expected result
		ObjectIntOpenHashMap<String> expected = new ObjectIntOpenHashMap<String>();
		expected.putOrAdd( "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", 4, 4 );
		expected.putOrAdd( "http://example.org/property1", 0, 0 );
		expected.putOrAdd( "http://www.w3.org/2000/01/rdf-schema#subPropertyOf", 1, 1 );
		expected.putOrAdd( "http://www.w3.org/2000/01/rdf-schema#subClassOf", 1, 1 );
		expected.putOrAdd( "http://www.w3.org/2000/01/rdf-schema#label", 4, 4 );
		expected.putOrAdd( "http://example.org/property2", 0, 0 );
		// run extraction
		try {
			// setup
			InputStream inStream = new FileInputStream( inFile );
			PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
			PipedRDFStream<Triple> rdfStream = new PipedTriplesStream( iter );
			// run
			RDFDataMgr.parse(rdfStream,	inStream, "", RDFLanguages.resourceNameToLang( inFile ) );
			VoidExtractor extractor = new VoidExtractor();
			extractor.extract( iter );
			// print
			System.out.println( "5.nt:\n  PROPERTIES: " + extractor.getCountedProperties().toString() );
			// test
			assertEquals( expected, extractor.getCountedProperties() );
		}
		catch( Exception e ) {
			fail( e.toString() );
		}
	}

}
