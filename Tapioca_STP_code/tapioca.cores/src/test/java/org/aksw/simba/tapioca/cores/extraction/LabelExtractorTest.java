/**
 * 
 */
package org.aksw.simba.tapioca.cores.extraction;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aksw.simba.tapioca.cores.extraction.LabelExtractor;
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
public class LabelExtractorTest {
	
	/**
	 * Test method for LabelExtractor#getLabels(), 0.nt
	 */
	@Test
	public final void testGetLabels0() {
		// set input file
		String inFile = "src/test/data/0.nt";
		// set uris
        Set<String> uris = new HashSet<String>();
        uris.add( "http://example.org/entity1" );
        uris.add( "http://example.org/Class1" );
        uris.add( "http://example.org/Class2" );
		
		// set expected result
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>();
        	// Entity 1
        	Set<String> e1 = new HashSet<String>();
        	e1.add( "entity 1" );
        	expected.put( "http://example.org/entity1", e1 );
			
		// try to run extraction
		try{
			// create input stream
			InputStream inStream = new FileInputStream( inFile );
			// create iterator
	        PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
	        // create RDF stream
	        PipedRDFStream<Triple> rdfStream = new PipedTriplesStream( iter );
	        // run the streamer
	        RDFDataMgr.parse(rdfStream,	inStream, "", RDFLanguages.resourceNameToLang( inFile ) );
	        // create extractor
	        LabelExtractor extractor = new LabelExtractor( uris );
	        // run extraction
	        extractor.extract( iter );
		        // print expected
		        System.out.println( "0, EXP: " + expected.toString() );
		        // print result
		        System.out.println( "0, RES: " + extractor.getLabels().toString() );
		        // check assertion
		        assertEquals( expected, extractor.getLabels() );
		}
		
		// handle exceptions
		catch( Exception e ) {
			fail( e.toString() );
		}
	}

	
	/**
	 * Test method for LabelExtractor#getLabels(), 3.nt
	 */
	@Test
	public final void testGetLabels3() {
		// set input file
		String inFile = "src/test/data/3.nt";
		// set uris
        Set<String> uris = new HashSet<String>();
        uris.add( "http://example.org/class1" );
        uris.add( "http://example.org/class2" );
        uris.add( "http://example.org/property1" );
        uris.add( "http://example.org/property2" );
		
		// set expected result
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>();
			
		// try to run extraction
		try{
			// create input stream
			InputStream inStream = new FileInputStream( inFile );
			// create iterator
	        PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
	        // create RDF stream
	        PipedRDFStream<Triple> rdfStream = new PipedTriplesStream( iter );
	        // run the streamer
	        RDFDataMgr.parse(rdfStream,	inStream, "", RDFLanguages.resourceNameToLang( inFile ) );
	        // create extractor
	        LabelExtractor extractor = new LabelExtractor( uris );
	        // run extraction
	        extractor.extract( iter );
		        // print expected
		        System.out.println( "3, EXP: " + expected.toString() );
		        // print result
		        System.out.println( "3, RES: " + extractor.getLabels().toString() );
		        // check assertion
		        assertEquals( expected, extractor.getLabels() );
		}
		
		// handle exceptions
		catch( Exception e ) {
			fail( e.toString() );
		}
	}

	
	/**
	 * Test method for LabelExtractor#getLabels(), 4.nt
	 */
	@Test
	public final void testGetLabels4() {
		// set input file
		String inFile = "src/test/data/4.nt";
		// set uris
        Set<String> uris = new HashSet<String>();
        uris.add( "http://rdfs.org/ns/void#entities" );
        uris.add( "http://rdfs.org/ns/void#class" );
        uris.add( "http://rdfs.org/ns/void#triples" );
        uris.add( "http://rdfs.org/ns/void#property" );
		
		// set expected result
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>();
			
		// try to run extraction
		try{
			// create input stream
			InputStream inStream = new FileInputStream( inFile );
			// create iterator
	        PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
	        // create RDF stream
	        PipedRDFStream<Triple> rdfStream = new PipedTriplesStream( iter );
	        // run the streamer
	        RDFDataMgr.parse(rdfStream,	inStream, "", RDFLanguages.resourceNameToLang( inFile ) );
	        // create extractor
	        LabelExtractor extractor = new LabelExtractor( uris );
	        // run extraction
	        extractor.extract( iter );
		        // print expected
		        System.out.println( "4, EXP: " + expected.toString() );
		        // print result
		        System.out.println( "4, RES: " + extractor.getLabels().toString() );
		        // check assertion
		        assertEquals( expected, extractor.getLabels() );
		}
		
		// handle exceptions
		catch( Exception e ) {
			fail( e.toString() );
		}
	}


	/**
	 * Test method for LabelExtractor#getLabels(), 5.nt
	 */
	@Test
	public final void testGetLabels5() {
		// set input file
		String inFile = "src/test/data/5.nt";
		// set uris
        Set<String> uris = new HashSet<String>();
        uris.add( "http://example.org/class1" );
        uris.add( "http://example.org/class2" );
        uris.add( "http://example.org/property1" );
        uris.add( "http://example.org/property2" );
		
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
			
		// try to run extraction
		try{
			// create input stream
			InputStream inStream = new FileInputStream( inFile );
			// create iterator
	        PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
	        // create RDF stream
	        PipedRDFStream<Triple> rdfStream = new PipedTriplesStream( iter );
	        // run the streamer
	        RDFDataMgr.parse(rdfStream,	inStream, "", RDFLanguages.resourceNameToLang( inFile ) );
	        // create extractor
	        LabelExtractor extractor = new LabelExtractor( uris );
	        // run extraction
	        extractor.extract( iter );
		        // print expected
		        System.out.println( "5, EXP: " + expected.toString() );
		        // print result
		        System.out.println( "5, RES: " + extractor.getLabels().toString() );
		        // check assertion
		        assertEquals( expected, extractor.getLabels() );
		}
		
		// handle exceptions
		catch( Exception e ) {
			fail( e.toString() );
		}
	}

}
