/**
 * Management of Metadata Extraction
 */
package org.aksw.simba.tapioca.metadataextraction;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * @author Kai
 *
 */
@FixMethodOrder( MethodSorters.NAME_ASCENDING )
public class MetadataExtractionMainTest {

	/**
	 * Test method for MetadataExtractionMain#main().
	 */
	@Test
	public final void test01Main() {
		String[] args = { "1", "2", "3" };
		System.out.println( "\n#01: Too little parameters" );
		assertFalse( MetadataExtractionMain.mainBool( args ) );		
	}

	/**
	 * Test method for MetadataExtractionMain#main().
	 */
	@Test
	public final void test02Main() {
		String[] args = { "1", "2", "3", "4", "5" };
		System.out.println( "\n#02: Too many parameters" );
		assertFalse( MetadataExtractionMain.mainBool( args ) );		
	}
	
	/**
	 * Test method for MetadataExtractionMain#main().
	 */
	@Test
	public final void test03Main() {
		String[] args = { "src/test/data/6.nt", "/tmp/6_out.nt", "N-TRIPLES", "http://example.org/testdata#5.nt" };
		System.out.println( "\n#03: Unreadable input file" );
		assertFalse( MetadataExtractionMain.mainBool( args ) );		
	}
	
	/**
	 * Test method for MetadataExtractionMain#main().
	 */
	@Test
	public final void test04Main() {
		String[] args = { "src/test/data/5.nt", "/tmp/5_out.nt", "XYZ", "http://example.org/testdata#5.nt" };
		System.out.println( "\n#04: Invalid output format" );
		assertFalse( MetadataExtractionMain.mainBool( args ) );		
	}

	/**
	 * Test method for MetadataExtractionMain#main().
	 */
	@Test
	public final void test05Main() {
		String[] args = { "src/test/data/5.nt", "/5_out.nt", "N-TRIPLES", "http://example.org/testdata#5.nt" };
		System.out.println( "\n#05: Invalid output file" );
		assertFalse( MetadataExtractionMain.mainBool( args ) );		
	}

	/**
	 * Test method for MetadataExtractionMain#main().
	 */
	@Test
	public final void test06Main() {
		String[] args = { "src/test/data/5.nt", "/tmp/5_out.nt", "N-TRIPLES", "http://example.org/testdata#5.nt" };
		System.out.println( "\n#05: Successful extraction" );
		assertTrue( MetadataExtractionMain.mainBool( args ) );		
	}
	
	/**
	 * Test method for MetadataExtractionMain#main().
	 */
	@Test
	public final void test07Main() {
		String[] args = { "src/test/data/7.rdf", "/tmp/7_out.rdf", "N-TRIPLES", "http://example.org/testdata#7.rdf" };
		System.out.println( "\n#07: Successful extraction, larger file" );
		assertTrue( MetadataExtractionMain.mainBool( args ) );		
	}

}
