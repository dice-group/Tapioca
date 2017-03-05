/**
 * Management of Metadata Extraction
 */
package org.aksw.simba.tapioca.metadataextraction;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * @author Kai
 *
 */
@FixMethodOrder( MethodSorters.NAME_ASCENDING )
public class MetadataExtractionManagerTest {

	/**
	 * Input file
	 */
	private static String inFile;
	
	/**
	 * Dataset URI
	 */
	private static String datasetUri;
	
	/**
	 * Path to output folder;
	 */
	private static String outPath;
	
	/**
	 * @throws java.lang.Exception An exception.
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		inFile = "src/test/data/5.nt";
		outPath = "/tmp/";
		datasetUri = "http://example.org/testdata#5.nt";
	}

	/**
	 * @throws java.lang.Exception
	 * Throws an exception.
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		inFile = null;
		outPath = null;
		datasetUri = null;
	}

	/**
	 * Test method for MetadataExtractionManager#run().
	 */
	@Test
	public final void test0501Run() {
		// set parameter
		String outFile = outPath + "5_out.nt";
		String outFormat = "N-TRIPLES";
		// run extraction
		MetadataExtractionManager mgr = new MetadataExtractionManager( inFile, outFile, outFormat, datasetUri );
		assertTrue( mgr.run() );
	}

	/**
	 * Test method for MetadataExtractionManager#run().
	 */
	@Test
	public final void test0502Run() {
		// set parameter
		String outFile = outPath + "5_out.ttl";
		String outFormat = "TURTLE";
		// run extraction
		MetadataExtractionManager mgr = new MetadataExtractionManager( inFile, outFile, outFormat, datasetUri );
		assertTrue( mgr.run() );
	}
	
	/**
	 * Test method for MetadataExtractionManager#run().
	 */
	@Test
	public final void test0503Run() {
		// set parameter
		String outFile = outPath + "5_out.rdf";
		String outFormat = "RDF/XML";
		// run extraction
		MetadataExtractionManager mgr = new MetadataExtractionManager( inFile, outFile, outFormat, datasetUri );
		assertTrue( mgr.run() );
	}
	
	/**
	 * Test method for MetadataExtractionManager#run().
	 */
	@Test
	public final void test0504Run() {
		// set parameter
		String outFile = outPath + "5_out.jsonld";
		String outFormat = "JSON-LD";
		// run extraction
		MetadataExtractionManager mgr = new MetadataExtractionManager( inFile, outFile, outFormat, datasetUri );
		assertTrue( mgr.run() );
	}
	
	/**
	 * Test method for MetadataExtractionManager#run().
	 */
	@Test
	public final void test0505Run() {
		// set parameter
		String outFile = outPath + "5_out.rj";
		String outFormat = "RDF/JSON";
		// run extraction
		MetadataExtractionManager mgr = new MetadataExtractionManager( inFile, outFile, outFormat, datasetUri );
		assertTrue( mgr.run() );
	}
	
	/**
	 * Test method for MetadataExtractionManager#run().
	 */
	@Test
	public final void test0506Run() {
		// set parameter
		String outFile = outPath + "5_out.xyz";
		String outFormat = "XYZ";
		// run extraction
		MetadataExtractionManager mgr = new MetadataExtractionManager( inFile, outFile, outFormat, datasetUri );
		assertFalse( mgr.run() );
	}
	
	/**
	 * Test method for MetadataExtractionManager#run().
	 */
	@Test
	public final void test0507Run() {
		// set parameter
		String outFile = "/5_out.nt";
		String outFormat = "N-TRIPLES";
		// run extraction
		MetadataExtractionManager mgr = new MetadataExtractionManager( inFile, outFile, outFormat, datasetUri );
		assertFalse( mgr.run() );
	}
	
	/**
	 * Test method for MetadataExtractionManager#run().
	 */
	@Test
	public final void test0601Run() {
		// set parameter
		String inFile = "src/test/data/6.nt";
		String outFile = "/6_out.nt";
		String outFormat = "N-TRIPLES";
		// run extraction
		MetadataExtractionManager mgr = new MetadataExtractionManager( inFile, outFile, outFormat, datasetUri );
		assertFalse( mgr.run() );
	}

}
