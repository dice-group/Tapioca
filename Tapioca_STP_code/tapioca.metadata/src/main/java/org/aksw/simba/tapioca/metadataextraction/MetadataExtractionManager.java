/**
 * Management of meta data Extraction
 */
package org.aksw.simba.tapioca.metadataextraction;

import java.io.FileOutputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * The handler for the extraction. It manages the I/O and the extraction task.
 * 
 * @author Kai, Kim
 *
 */
public class MetadataExtractionManager {

	// -------------------------------------------------------------------------
	// ------------- Variables -------------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Logger for errors, warnings and other informations.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(MetadataExtractionManager.class);

	/**
	 * The path of the input file or input folder as string.
	 */
	private String inFile;

	/**
	 * The output folder as string.
	 */
	private String outFile;

	/**
	 * Output format -> JSON-LD -> N-TRIPLES -> N3 -> RDF/JSON -> RDF/XML ->
	 * RDF/XML-ABBREV -> TURTLE
	 */
	private String outFormat;

	/**
	 * URI of the input data set
	 */
	private String datasetUri;

	/**
	 * Constructor
	 * 
	 * @param inFile
	 *            Input file
	 * @param outFile
	 *            Output file
	 * @param outFormat
	 *            output Format
	 * @param datasetUri
	 *            URI of the data set
	 */
	public MetadataExtractionManager(String inFile, String outFile, String outFormat, String datasetUri) {
		this.inFile = inFile;
		this.outFile = outFile;
		this.outFormat = outFormat;
		this.datasetUri = datasetUri;
	}

	/**
	 * Execute meta data extraction
	 * 
	 * @return TRUE or FALSE
	 */
	public boolean run() {
		try {
			// count URIs
			VoidExtractionHandler vHandler = new VoidExtractionHandler();
			Model voidModel = vHandler.extractVoidInfo(datasetUri, inFile);
			// count labels
			LabelExtractionHandler lHandler = new LabelExtractionHandler();
			lHandler.setVoidModel(voidModel);
			voidModel = lHandler.extractLabels(inFile);
			// write output file
			OutputStream os = new FileOutputStream(outFile);
			voidModel.write(os, outFormat);
			// return success
			return true;
		} catch (Exception e) {
			// return error
			LOGGER.error(e.toString());
			return false;
		}
	}

}
