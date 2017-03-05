/**
 * Management of meta data Extraction
 */
package org.aksw.simba.tapioca.metadataextraction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The user interface for the meta data extraction. It handles the command line
 * input of the user and starts the extraction.
 * 
 * @author Kai, Kim
 *
 */
public class MetadataExtractionMain {

	// -------------------------------------------------------------------------
	// ------------- Variables -------------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Logger for errors, warning and other informations.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(MetadataExtractionMain.class);

	/**
	 * A message, which is shown when the user set the "-help" or "--help" flag.
	 */
	final static String HELP = "Usage:\n"
			+ "Parameters: \n "
			+ "\t<input file>    -  path of your RDF input dataset \n"
			+ "\t<output file>   -  output path where the result of metadataExtraction will be saved to\n"
			+ "\t\t\t   including the output file name e.g. home/extraction/output.nt\n"
			+ "\t\t\t   Even though it is not mandatory to match the filename extension\n"
			+ "\t\t\t   with the specified output format, it is recommended to do so.\n"
			+ "\t<output format> -  choose one of the listed formats for the output file,\n"
			+ "\t\t\t   not necessarily the input format, you can cast between the different RDF serializations\n"
			+ "\t<dataset uri>   -  the URI of your RDF input dataset e.g. the link from which you downloaded the dataset \n\n"
			+ "Possible input and out formats:\n" + "  -> JSON-LD\n" + "  -> N-TRIPLES\n" + "  -> N3\n"
			+ "  -> RDF/JSON\n" + "  -> RDF/XML\n" + "  -> RDF/XML-ABBREV\n" + "  -> TURTLE";

	// -------------------------------------------------------------------------
	// ------------- Methods ---------------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * The entry point for the meta data extraction. The user's command line
	 * arguments are saved in args.
	 * 
	 * @param args
	 *            Command line parameters
	 */
	public static void main(String[] args) {
		// need exactly 4 parameters
		if (args.length == 4) {
			// try it
			try {
				// Create an extraction manager
				MetadataExtractionManager mgr = new MetadataExtractionManager(args[0], args[1], args[2], args[3]);
				// Run extraction
				if (mgr.run()) {
					// print success
					LOGGER.info("\nSuccess:\n  Saved result to " + args[1] + " .");
				} else {
					// print error
					LOGGER.error("\nError:\n  Running metadata extraction failed.");
				}
			}
			// handle exception
			catch (Exception e) {
				LOGGER.error("\nError:\n  Initiating extraction process failed.");
			}
		}

		// --help and -help flag
		else if (args.length == 1 && (args[0].equals("--help") || args[0].equals("-help"))) {
			LOGGER.info(HELP);
		}
		// not exactly 4 arguments
		else {
			LOGGER.error("\nError:\n" + "  Wrong number of arguments.\n\n" + HELP);
		}

	}

	@Deprecated
	/**
	 * This main has the same functionality as the standard main. It's only used
	 * for the JUNIT tests.
	 * 
	 * @param args
	 *            Command line parameters
	 * @return TRUE if data has been extracted.
	 */
	public static boolean mainBool(String[] args) {
		// need exactly 4 parameters
		if (args.length == 4) {
			// try it
			try {
				// Create an extraction manager
				MetadataExtractionManager mgr = new MetadataExtractionManager(args[0], args[1], args[2], args[3]);
				// Run extraction
				if (mgr.run()) {
					// print success
					LOGGER.info("\nSuccess:\n  Saved result to " + args[1] + " .");
					return true;
				} else {
					// print error
					LOGGER.error("\nError:\n  Running metadata extraction failed.");
					return false;
				}
			}
			// handle exception
			catch (Exception e) {
				LOGGER.error("\nError:\n  Initiating extraction process failed.");
				return false;
			}
		}
		// --help and -help flag
		else if (args[0] == "--help" || args[0] == "-help") {
			System.out.println(HELP);
			return true;
		}
		// not exactly 4 arguments
		else {
			LOGGER.error("\nError:\n" + "  Wrong number of arguments.\n\n" + HELP);
			return false;
		}
	}
}
