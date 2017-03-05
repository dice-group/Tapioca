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
import java.io.FileNotFoundException;

import org.aksw.simba.tapioca.cores.preprocessing.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for the TM (Topic Modeling) based index generation.
 *
 */
public class TMBasedIndexGenerator {

	// -------------------------------------------------------------------------
	// ------------------ Variables --------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Logger for errors, warnings and other informations.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(TMBasedIndexGenerator.class);

	/**
	 * MetadataFile for Final Corpus Creation needs to be saved to
	 * /src/main/resources/
	 */
	public static final String META_DATA_FILE = "src" + File.separator + "main" + File.separator + "resources"
			+ File.separator + "lodStats" + File.separator + "datasets.nt";

	/**
	 * MetadataFile for Final Corpus Creation needs to be saved to
	 * /src/main/resources
	 */
	public static final String STAT_RESULT_FILE = "src" + File.separator + "main" + File.separator + "resources"
			+ File.separator + "lodStats" + File.separator + "statresult.nt";

	/**
	 * MetadataFile contains descriptions and keywords needs to be saved to
	 * /src/main/resources
	 */
	public static final String MODEL_META_DATA_FILE = "src" + File.separator + "main" + File.separator + "resources"
			+ File.separator + "lodStats" + File.separator + "lodstats.nt";

	/**
	 * Identifier for the TMbased model file.
	 */
	public static final String MODEL_FILE = "probAlgState.object";

	/**
	 * The final TMbased corpus file.
	 */
	public static String finalCorpusFile = InitialCorpusCreation.CORPUS_NAME + "_final.corpus";

	/**
	 * The initial corpus creation.
	 */
	private static InitialCorpusCreation initialCreation;

	// -------------------------------------------------------------------------
	// ------------------ Methods ----------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Run the TMBased Index Generation
	 */
	public void run() {
		// write output to the model folder
		File outputFolder = new File(IndexGeneratorMain.modelFolder);
		if (!outputFolder.exists()) {
			outputFolder.mkdirs();
			LOGGER.info("Created model folder to " + outputFolder.getPath());
		} else {
			LOGGER.info("ModelFolder already exists at " + outputFolder.getPath());
		}

		// create the final corpus file
		File datasetDescriptionsFile = new File(IndexGeneratorMain.modelFolder + File.separator + finalCorpusFile);
		if (datasetDescriptionsFile.exists()) {
			LOGGER.info("The final corpus file already exists.");
		} else {
			generateFinalCorpusFile();
			LOGGER.info("Final corpus file was successfully saved to " + datasetDescriptionsFile.getPath());
		}

		// create and generate the model file
		File modelFile = new File(IndexGeneratorMain.modelFolder + File.separator + MODEL_FILE);
		if (modelFile.exists()) {
			LOGGER.info("The model file already exists.");
		} else {
			generateModel();
			LOGGER.info("The model file was successfully saved to " + modelFile.getPath());
		}
	}

	/**
	 * Generate the first corpus file, running InitialCorpusCreation.
	 */
	protected static void generateFirstCorpusFile() {
		initialCreation = new InitialCorpusCreation();
		initialCreation.run(new File(IndexGeneratorMain.inputFolder), IndexGeneratorMain.uriUsage,
				IndexGeneratorMain.wordOccurence);
	}

	/**
	 * Generate LDA corpus file.
	 */
	protected void generateLDACorpusFile() {
		File corpusFile = new File(InitialCorpusCreation.corpusFile);
		if (!corpusFile.exists()) {
			LOGGER.info("The first corpus file is not existing. Trying to generate it...");
			generateFirstCorpusFile();
			if (!corpusFile.exists()) {
				LOGGER.error("The first corpus file is not existing and couldn't be generated.");
				return;
			}
		}

		LDACorpusCreation creation = new LDACorpusCreation(IndexGeneratorMain.uriUsage,
				IndexGeneratorMain.wordOccurence);

		WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever;
		cachingLabelRetriever = new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null,
				initialCreation.CACHE_FILES, new File[0]);
		creation.run(cachingLabelRetriever);
		cachingLabelRetriever.close();
	}

	/**
	 * Generate the final TMbased corpus file.
	 */
	protected void generateFinalCorpusFile() {
		if (checkLDACorpusExistence()) {
			MetaDataInformationCollector collector = new MetaDataInformationCollector();
			LOGGER.info("Generating final corpus file...");
			try {
				collector.run(META_DATA_FILE, ModelGenerator.LDA_CORPUS_FILE, STAT_RESULT_FILE,
						IndexGeneratorMain.modelFolder + File.separator + finalCorpusFile, MODEL_META_DATA_FILE);
			} catch (FileNotFoundException e) {
				LOGGER.error(
						"Could not load additional metadata from files. Check location of datasets.nt, lodstats.nt and statresult.nt. Aborting. ");
				System.exit(1);
			}
		}
	}

	/**
	 * Generate TMbased model.
	 */
	protected void generateModel() {
		if (checkLDACorpusExistence()) {
			ModelGenerator generator = new ModelGenerator(IndexGeneratorMain.algorithm);
			LOGGER.info("Generating Model file...");
			generator.run();
		}
	}

	/**
	 * Check if the LDA object file exists.
	 * 
	 * @return true if LDA corpus exists or could be generated
	 */
	protected boolean checkLDACorpusExistence() {
		File ldaCorpusFile = new File(ModelGenerator.LDA_CORPUS_FILE);
		if (!ldaCorpusFile.exists()) {
			LOGGER.warn("The LDA corpus file is not existing. Trying to generate it...");
			generateLDACorpusFile();
			if (!ldaCorpusFile.exists()) {
				LOGGER.error("The LDA corpus file is not existing and couldn't be generated.");
				return false;
			}
		}
		return true;
	}
}