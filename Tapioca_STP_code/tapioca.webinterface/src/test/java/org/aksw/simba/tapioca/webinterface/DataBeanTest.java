package org.aksw.simba.tapioca.webinterface;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.aksw.simba.tapioca.cores.data.Dataset;
import org.aksw.simba.tapioca.server.data.SearchResult;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * PLEASE NOTE: In order to execute this test, you need to comment out
 * line 53 to 56 in SearchEngineBean.java
 *  
 * @author Kai F.
 *
 */
@FixMethodOrder( MethodSorters.NAME_ASCENDING )
public class DataBeanTest {
	
	/**
	 * Data bean
	 */
	private static DataBean bean;
	
	/**
	 * Model
	 */
	private static Model model;
	
	/**
	 * Search results
	 */
	private static List<SearchResult> results;
	
	/**
	 * Search engine bean
	 */
	private static SearchEngineBean engine;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// create bean
		bean = new DataBean();
		
		// set text
		bean.setInputText( "Hello world." );
		
		// set model
		InputStream stream = new FileInputStream( new File( "src/test/data3/metadata/34.nt" ) );
		model = ModelFactory.createDefaultModel();
		RDFDataMgr.read( model, stream, RDFLanguages.nameToLang( "N-TRIPLES" ) );
		bean.setInputModel( model );
		stream.close();
		
		// set search results
		results = new ArrayList<SearchResult>();
		results.add( new SearchResult( new Dataset( "dataset1", "uri1" ), 0.3 ) );
		results.add( new SearchResult( new Dataset( "dataset2", "uri2" ), 0.7 ) );
		results.add( new SearchResult( new Dataset( "dataset3", "uri3" ), 1.0 ) );
		bean.setSearchResults( results );
		
		// set search engine bean
		String[] cachefiles = { "src/test/data3/index/cache/uriToLabelCache_1.object",
				"src/test/data3/index/cache/uriToLabelCache_2.object", 
				"src/test/data3/index/cache/uriToLabelCache_3.object"
		};
		String modelfile = "src/test/data3/index/model/probAlgState.object";
		String corpusfile = "src/test/data3/index/model/lodStats_final_noFilter.corpus";
		String rdfMetaDataFile = "src/test/data3/index/model/lodstats.nt";
		int elasticport = 9300;
		engine = new SearchEngineBean( cachefiles, modelfile, corpusfile, rdfMetaDataFile, elasticport );
		bean.setSearchEngine( engine );
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		bean = null;
		model = null;
		results = null;
		engine = null;
	}

	@Test
	public final void test01GetInputText() {
		assertEquals( "Hello world.", bean.getInputText() );
	}

	@Test
	public final void test02GetInputModel() {
		assertTrue( bean.getInputModel().isIsomorphicWith( model ) );
	}

	@Test
	public final void test03GetSearchResults() {
		assertEquals( results, bean.getSearchResults() );
	}
	
	@Test
	public final void test04GetSearchEngine() {
		assertTrue( bean.getSearchEngine().equals( engine ) );
	}

	@Test
	public final void test05ReadFromModel() {
		assertTrue( bean.readFromModel() );
	}

	@Test
	public final void test06Result() {
		// text search, no result
		bean.setInputText( "beer" );
		assertEquals( "/result.xhtml", bean.result() );
		assertEquals( bean.getSearchResults().size(), 3 );
	}

}
