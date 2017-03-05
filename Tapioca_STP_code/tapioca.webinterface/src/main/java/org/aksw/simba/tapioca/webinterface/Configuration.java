package org.aksw.simba.tapioca.webinterface;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration {

	/**
	 * Logging
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger( Configuration.class );
	
	/**
	 * List of cache files
	 */
    private List<String> cachefiles;

    /**
     * Model file
     */
    private String modelfile;
    
    /**
     * Corpus file
     */
    private String corpusfile;

    /**
     * Metadata information file
     */
    private String metadatafile;
    
    /**
     * Port Elasticsearch server is running on
     */
    private int elasticport;

    /**
     * Getter
     * @return Cache files
     */
    protected String[] getCachefiles() {
            String[] cachefilesArray = new String[ cachefiles.size() ];
            cachefilesArray = cachefiles.toArray( cachefilesArray );
            return cachefilesArray;
    }

    /**
     * Getter
     * @return Model folder
     */
	protected String getModelfile() {
		return modelfile;
	}
	
	/**
	 * Getter
	 * @return Corpus file
	 */
	protected String getCorpusfile() {
		return corpusfile;
	}

	/**
	 * Getter
	 * @return Metadata information file
	 */
	protected String getMetadatafile() {
		return metadatafile;
	}
	
	/**
	 * Getter
	 * @return Elasticsearch server port
	 */
	protected int getElasticport() {
		return elasticport;
	}
	
	/**
	 * Constructor
	 */
	public Configuration() {
		Properties properties = new Properties();
		ClassLoader loader = Configuration.class.getClassLoader();
        if( loader==null ) {
        	loader = ClassLoader.getSystemClassLoader();
        }
		try {
			cachefiles = new ArrayList<String>();
			properties.load( loader.getResourceAsStream( "tapioca.properties" ) );
			cachefiles.add( properties.getProperty( "cachefiles0" ) );
			cachefiles.add( properties.getProperty( "cachefiles1" ) );
			cachefiles.add( properties.getProperty( "cachefiles2" ) );
			modelfile = properties.getProperty( "modelfile" );
			corpusfile = properties.getProperty( "corpusfile" );
			metadatafile = properties.getProperty( "metadatafile" );
			elasticport = new Integer( properties.getProperty( "elasticport" ) );
		} catch( Exception e ) {
			LOGGER.error( "Failed loading tapioca.properties", e );
		}
	}
	
}
