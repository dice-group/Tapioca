/**
 * This file is part of tapioca.indexgenerator.
 *
 * tapioca.indexgenerator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.indexgenerator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.indexgenerator.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.indexgenerator.docgen;

import java.io.File;
import java.io.IOException;

import org.aksw.simba.tapioca.cores.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.WordOccurence;
import org.aksw.simba.tapioca.cores.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.topicmodeling.algorithm.mallet.MalletLdaWrapper;
import org.aksw.simba.topicmodeling.algorithms.ModelingAlgorithm;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains the entry point for the index generation. It handles the
 * users command line input and manages the file input and output.
 * 
 * @author Marleen W., Duong T.D.
 */
public class IndexGeneratorMain {

	// -------------------------------------------------------------------------
	// ------------------ Variables --------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Logger for errors, warnings and other informations.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(IndexGeneratorMain.class);

	/**
	 * defines which URIs will be considered classes, properties or all -
	 * depending on User
	 */
	protected static UriUsage uriUsage;

	/**
	 * defines in which way the WordOccurence will be used unique or log -
	 * depending on User
	 */
	protected static WordOccurence wordOccurence;

	/**
	 * The algorithm that will be used
	 */
	public static ModelingAlgorithm algorithm;

	// ------------------ Folder and files ------------------------------------

	/**
	 * Path of the input folder, which contains the meta data files.
	 */
	public static String inputFolder;

	/**
	 * Output folder (created depending on input folder).
	 */
	public static String outputFolder;

	/**
	 * The folder the label cache files are saved to (nested in output folder).
	 */
	public static String cacheFolder;

	/**
	 * The folder the model files are saved to.
	 */
	public static String modelFolder;

	/**
	 * Number of topics used by the algorithm.
	 */
	public static int numberOfTopics;

	// ------------------ Messages ---------------------------------------------

	/**
	 * Usage help message.
	 */
	protected final static String USAGE = "Possible uriUsage: \n"
			+ "\tclasses\n\tproperties\n\tall\n\teclasses\n\teall\n" + "Possible wordOccurrences: \n"
			+ "\tunique\n\tlog  \nPossible algorithm: \n\tLDA";
	/**
	 * General help message.
	 */
	protected final static String HELP = "Usage:\n"
			+ " \" java -jar indexgenerator.jar <path/to/inputFolder> <uriUsage> <wordOccurence> <numberOfTopics> <algorithm>\"\n\n"
			+ USAGE;

	/**
	 * The error message.
	 */
	protected static final String ERROR = "Wrong number of arguments!\n\n" + HELP;

	// -------------------------------------------------------------------------
	// ------------------ Methods ----------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Start the document and index generation.
	 * 
	 * @param args
	 *            command line arguments.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		try {
			// checks for right amount of command line parameters, prints help
			// message
			if (args.length != 5) {
				if (args[0].equals("-help") || args[0].equals("--help")) {
					LOGGER.info(HELP);
				} else {
					LOGGER.info(ERROR);
				}
				return;
			}

			// creation of output folders
			inputFolder = args[0];
			if (inputFolder.endsWith(File.separator)) {
				inputFolder = inputFolder.substring(0, inputFolder.length() - 1);
			}
			boolean folderCreation = makeDirectory(inputFolder);

			// checking for valid output folders and command line parameters
			if (!folderCreation) {
				LOGGER.info("Creating output folders failed. Aborting.");
				return;
			} else if (!NumberUtils.isNumber(args[3])) {
				LOGGER.info("Not valid number of topics. Number of topics must be a natural number.");
				return;
			} else if (!args[4].toLowerCase().equals("lda")) {
				LOGGER.info("Algorithm " + args[4] + " is not implemented.");
				return;
			} else {

				// set UriUsage and WordOccurence and check for errors
				uriUsage = UriCountMappingCreatingDocumentSupplierDecorator.getEnum(args[1]);
				wordOccurence = StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.getEnum(args[2]);

				if (uriUsage == null || wordOccurence == null) {
					LOGGER.error("Could not set UriUsage and WordOccurence. Aborting.");
					return;
				}

				// Create corpus files for initial and LDA corpus
				InitialCorpusCreation.corpusFile = outputFolder + File.separator + InitialCorpusCreation.CORPUS_NAME
						+ ".corpus";
				LDACorpusCreation.corpusFile = outputFolder + File.separator + LDACorpusCreation.CORPUS_NAME
						+ ".corpus";

				// running InitialCorpusCreation
				InitialCorpusCreation initial = new InitialCorpusCreation();

				LOGGER.info("Save result to:" + "\n\t" + outputFolder + "\n\t" + modelFolder);
				LOGGER.info("Creating initial corpus...");
				initial.run(new File(inputFolder), uriUsage, wordOccurence);

				// running model generation with given algorithm and number of
				// steps
				numberOfTopics = NumberUtils.toInt(args[3]);
				algorithm = new MalletLdaWrapper(numberOfTopics);
				ModelGenerator modelGenerator = new ModelGenerator(algorithm);
				LOGGER.info("Creating model...");
				modelGenerator.run();

				// running TMBasedIndexgenerator
				TMBasedIndexGenerator tmGenerator = new TMBasedIndexGenerator();
				tmGenerator.run();
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("Fatal Error occured. Aborting...");
		}
	}

	/**
	 * Check if the file name is valid.
	 * 
	 * @param file
	 *            path that needs to be verified
	 * @return true when given String is a valid path
	 */
	public static boolean isFilenameValid(String file) {
		File f = new File(file);
		return f.exists();
	}

	/**
	 * Create new output folders depending on the path of the input folder.
	 * 
	 * @param input
	 *            path of input folder
	 * @return true if folders were successfully created
	 */
	public static boolean makeDirectory(String input) {

		if (isFilenameValid(input)) {

			File outputfile = new File(FilenameUtils.getFullPath(inputFolder) + FilenameUtils.getName(inputFolder)
					+ "_IndexGenerator_Output");
			File labelcachefile = new File(FilenameUtils.getFullPath(inputFolder) + FilenameUtils.getName(inputFolder)
					+ "_IndexGenerator_Output" + File.separator + "label-cache");
			File modelfile = new File(FilenameUtils.getFullPath(inputFolder) + FilenameUtils.getName(inputFolder)
					+ "_IndexGenerator_Output" + File.separator + InitialCorpusCreation.CORPUS_NAME + "_model");

			if (!outputfile.isDirectory()) {
				boolean success = outputfile.mkdir() && modelfile.mkdir() && labelcachefile.mkdir();
				if (success) {
					LOGGER.info("Created new output folders to:" + "\n\t" + outputfile.getPath() + "\n\t"
							+ labelcachefile.getPath() + "\n\t" + modelfile.getPath());
					outputFolder = outputfile.getPath();
					cacheFolder = labelcachefile.getPath();
					modelFolder = modelfile.getPath();
					return true;
				} else {
					LOGGER.error("Could not create output folders");
					return false;
				}

			} else if (outputfile.isDirectory() && (!labelcachefile.isDirectory() || !modelfile.isDirectory())) {
				boolean successLabel = labelcachefile.mkdir();
				boolean successModel = modelfile.mkdir();

				if (successLabel && successModel) {
					outputFolder = outputfile.getPath();
					cacheFolder = labelcachefile.getPath();
					modelFolder = modelfile.getPath();
					LOGGER.info("Created missing nested folders to: " + "\n\t" + cacheFolder + "\n\t" + modelFolder);
					return true;
				} else if (successLabel) {
					outputFolder = outputfile.getPath();
					modelFolder = modelfile.getPath();
					cacheFolder = labelcachefile.getPath();
					LOGGER.info("Created missing labelcache folder to: " + "\n\t" + cacheFolder);
					return true;
				} else if (successModel) {
					outputFolder = outputfile.getPath();
					cacheFolder = labelcachefile.getPath();
					modelFolder = modelfile.getPath();
					LOGGER.info("Created missing model folder to: " + "\n\t" + modelFolder);
					return true;
				} else {
					LOGGER.error("Could not create output folders");
					return false;
				}

			} else {
				LOGGER.info("Path already exists. Results saved to:" + "\n\t" + outputfile.getPath() + "\n\t"
						+ labelcachefile.getPath() + "\n\t" + modelfile.getPath());
				outputFolder = outputfile.getPath();
				cacheFolder = labelcachefile.getPath();
				modelFolder = modelfile.getPath();
				return true;
			}
		} else {
			LOGGER.error("\nGiven String is not a valid path. Choose an existing input folder.");
			return false;
		}
	}
}
