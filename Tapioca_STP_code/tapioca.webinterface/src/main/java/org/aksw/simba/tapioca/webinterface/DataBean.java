package org.aksw.simba.tapioca.webinterface;

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import javax.faces.component.UIComponent;
import javax.faces.context.FacesContext;
import javax.faces.validator.ValidatorException;
import javax.servlet.http.Part;

import org.aksw.simba.tapioca.server.data.SearchResult;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentURI;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * 
 * Managed bean, implements communication with the HTML frontend 
 * of the web application. It acts as the "Controller" part within
 * the MVC design pattern of the application
 * 
 * @author Kai
 *
 */
@ManagedBean( name = "dataBean" )
@RequestScoped
public class DataBean implements Serializable{

	private static final long serialVersionUID = 1L;

	/**
	 * Logging
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger( DataBean.class );
	
	/**
	 * Input string
	 */
	private String inputText;
    
	/**
	 * Uploaded file
	 */
	private Part inputFile;
	
	/**
	 * Uploaded file, converted to a Jena model
	 */
	private Model inputModel;
	
	/**
	 * List of data sets, representing the response to the query
	 */
	private List<SearchResult> searchResults = new ArrayList<SearchResult>();

	/**
	 * Message when search did not match anything
	 */
	private String emptyResultMessage;

	/**
	 * Search engine bean
	 */
	@ManagedProperty("#{searchEngineBean}")
	private SearchEngineBean searchEngine;
	
	/**
	 * Constructor
	 */
	public DataBean() {
	}
	
	/**
	 * Get input string
	 * @return input string
	 */
	public String getInputText() {
		return inputText;
	}

	/**
	 * Set input string
	 * @param inputText input string
	 */
	public void setInputText(String inputText) {
		this.inputText = inputText;
	}
	
	/**
	 * Get uploaded file
	 * @return uploaded file
	 */
	public Part getInputFile() {
		return inputFile;
	}

	/**
	 * Set uploaded file
	 * @param inputFile uploaded file
	 */
	public void setInputFile(Part inputFile) {
		this.inputFile = inputFile;
	}
	
	/**
	 * Get input model
	 * @return input model
	 */
	public Model getInputModel() {
		return inputModel;
	}

	/**
	 * Set input model
	 * @param inputModel input model
	 */
	public void setInputModel(Model inputModel) {
		this.inputModel = inputModel;
	}

	/**
	 * get list of data sets
	 * @return list of data sets
	 */
	public List<SearchResult> getSearchResults() {
		return searchResults;
	}

	/**
	 * Set list of data sets
	 * @param datasets list of data sets
	 */
	public void setSearchResults(List<SearchResult> searchResults) {
		this.searchResults = searchResults;
	}
	
	/**
	 * get message when search did not match
	 * @return emptyResultMessage
	 */
	public String getEmptyResultMessage() {
		return emptyResultMessage;
	}
	
	/**
	 * Get search engine bean
	 * @return Search engine bean
	 */
	public SearchEngineBean getSearchEngine() {
		return searchEngine;
	}

	/**
	 * Set search engine bean
	 * @param Search engine bean
	 */
	public void setSearchEngine(SearchEngineBean searchEngine) {
		this.searchEngine = searchEngine;
	}
	
	/**
	 * Convert uploaded file to a Jena model
	 */
	protected void inputFileToModel( String lang ) throws Exception {
		// convert to stream
		InputStream is = inputFile.getInputStream();
		// read it
		inputModel = ModelFactory.createDefaultModel();
		RDFDataMgr.read( inputModel, is, RDFLanguages.nameToLang( lang ) );
	}

	/**
	 * Read values from model
	 * @param property Property
	 * @return Read success
	 */
	protected boolean readFromModel() {
		try {
			Document document = searchEngine.createDocument(inputModel);
			LOGGER.info( "Successfully created a document." );
			LOGGER.info( "Dataset URI:" + document.getProperty( DocumentURI.class ).getStringValue() );
			return true;
		} catch( Exception e ) {
			LOGGER.info( "Failed to create document.", e );
			return false;
		}
	}	
	/**
	 * Validate the file to be uploaded
	 * @param ctx FacesContext - the specific JSP page
	 * @param comp UIComponent - the "h:inputText" component
	 * @param value Object - the uploaded file itself
	 */
	public void validateFile( FacesContext ctx, UIComponent comp, Object value ) {
		// set input file
		inputFile = (Part) value;
		
		// convert to model
		try {
			inputFileToModel( "JSON-LD" );
		} catch( Exception e1 ) {
			try {
				inputFileToModel( "N-TRIPLES" );
			} catch( Exception e2 ) {
				try {
					inputFileToModel( "N3" );
				} catch( Exception e3 ) {
					try {
						inputFileToModel( "RDF/JSON" );
					} catch( Exception e4 ) {
						try {
							inputFileToModel( "RDF/XML" );
						} catch( Exception e5 ) {
							try {
								inputFileToModel( "RDF/XML-ABBREV" );
							} catch( Exception e6 ) {
								try {
									inputFileToModel( "TURTLE" );
								} catch( Exception e7 ) {
									throw new ValidatorException( new FacesMessage( e7.getMessage() ) );
								}
							}
						}
					}
				}
				
			}
		}
		
		// read and test
		if( !( readFromModel() ) ) {			
			throw new ValidatorException( new FacesMessage( "Input error: VOID syntax probably corrupt." ) );
		}
	}
	
	/**
	 * Create a response to the query
	 * @return JSF page showing the result - result.xhtml
	 */
	public String result() {
		SearchEngineObserver eingineObserver = new SearchEngineObserver();
		SearchEngineBean.getTMEngine().setWorkProgress(0);
		eingineObserver.start();
		emptyResultMessage = "";		
		// search text
		if( inputFile == null ) {
			LOGGER.info( "Text" );
			try{
				searchResults = searchEngine.run( inputText );
		    } catch(Exception e) {
				if (!SearchEngineBean.getESEngine().isESRunning())
					emptyResultMessage = "No nodes available, please make sure that elasticsearch server is running.";
		        LOGGER.error("Got an Exception: ", e);
		    }	
		}
		// search rdf file
		else {
			LOGGER.info( "File" );
			searchResults = searchEngine.run(inputModel);
		}
		if (searchResults.isEmpty()){
			emptyResultMessage = "Search did not match any documents. " + emptyResultMessage;
		}
		// return result url
		return "/result.xhtml";
	}	
}
