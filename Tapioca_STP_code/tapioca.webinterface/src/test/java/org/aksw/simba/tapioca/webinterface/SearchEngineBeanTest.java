package org.aksw.simba.tapioca.webinterface;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * PLEASE NOTE: In order to execute this test, you need to comment out
 * line 49 to 52 in SearchEngineBean.java
 *  
 * @author Kai F.
 *
 */
public class SearchEngineBeanTest {
	
	/**
	 * Search engine bean
	 */
	private static SearchEngineBean engine;
	
	/**
	 * Input model
	 */
	private static Model model;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// create engine
		String[] cachefiles = { "src/test/data3/index/cache/uriToLabelCache_1.object",
				"src/test/data3/index/cache/uriToLabelCache_2.object", 
				"src/test/data3/index/cache/uriToLabelCache_3.object"
		};
		String modelfile = "src/test/data3/index/model/probAlgState.object";
		String corpusfile = "src/test/data3/index/model/lodStats_final_noFilter.corpus";
		String rdfMetaDataFile = "src/test/data3/index/model/lodstats.nt";
		int elasticport = 9300;
		engine = new SearchEngineBean( cachefiles, modelfile, corpusfile, rdfMetaDataFile, elasticport );
		
		// create model
		InputStream stream = new FileInputStream( new File( "src/test/data3/metadata/34.nt" ) );
		model = ModelFactory.createDefaultModel();
		RDFDataMgr.read( model, stream, RDFLanguages.nameToLang( "N-TRIPLES" ) );
		stream.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		engine = null;
	}

	@Test
	public final void testCreateDocument() {
		assertFalse( engine.createDocument( model ).toString().isEmpty() );
	}

	@Test
	public final void testRunString() {
		try {
			assertEquals( 3, engine.run( "beer" ).size() );
		} catch( Exception e ) {
			System.err.println( e );
		}
	}

	@Test
	public final void testRunModel() {
		try {
			assertEquals( engine.run( model ).size(), 20 );
		} catch( Exception e ) {
			System.err.println( e );
		}
	}

}
