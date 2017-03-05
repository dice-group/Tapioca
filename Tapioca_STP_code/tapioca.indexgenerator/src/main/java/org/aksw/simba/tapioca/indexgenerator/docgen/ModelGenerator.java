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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.aksw.simba.topicmodeling.algorithms.LDAModel;
import org.aksw.simba.topicmodeling.algorithms.ModelingAlgorithm;
import org.aksw.simba.topicmodeling.algorithms.ProbTopicModelingAlgorithmStateSupplier;
import org.aksw.simba.topicmodeling.io.CorpusReader;
import org.aksw.simba.topicmodeling.io.gzip.GZipCorpusObjectReader;
import org.aksw.simba.topicmodeling.io.gzip.GZipProbTopicModelingAlgorithmStateWriter;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentTextWordIds;
import org.aksw.simba.topicmodeling.utils.doc.DocumentWordCounts;
import org.aksw.simba.topicmodeling.utils.vocabulary.Vocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Class of IndexGenerator
 * 
 * @author Michael Roeder, Marleen W., Duong T.D.
 */

public class ModelGenerator {

	// -------------------------------------------------------------------------
	// ------------------ Variables --------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Logger for errors, warnings and other informations.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ModelGenerator.class);

	/**
	 * Number of steps for the algorithm.
	 */
	private static final int NUMBER_OF_STEPS = 1040;

	/**
	 * Path to the model object file.
	 */
	protected static String MODEL_OBJECT_FILE = IndexGeneratorMain.modelFolder + File.separator + "probAlgState.object";

	/**
	 * Path to the LDA corpus object file, which will be read in.
	 */
	protected static String LDA_CORPUS_FILE = LDACorpusCreation.ldaCorpusFile;

	/**
	 * The algorithm, which will be used.
	 */
	private ModelingAlgorithm algorithm;

	// -------------------------------------------------------------------------
	// ------------------ Methods ----------------------------------------------
	// ------------------------------------------------------------------------

	/**
	 * Constructor.
	 * 
	 * @param algorithm
	 *            The Algorithm
	 */
	public ModelGenerator(ModelingAlgorithm algorithm) {
		this.algorithm = algorithm;
	}

	/**
	 * Run the model generator.
	 */
	public void run() {
		// read in LDA corpus from File
		CorpusReader reader = new GZipCorpusObjectReader(new File(LDA_CORPUS_FILE));
		Corpus corpus = reader.getCorpus();
		if (corpus == null) {
			LOGGER.error("Couldn't load corpus from file. Aborting.");
			return;
		}
		// performing algorithm
		algorithm.initialize(corpus);
		for (int i = 0; i < NUMBER_OF_STEPS; ++i) {
			algorithm.performNextStep();
		}
		// write out result of algorithm
		File mFile = new File(MODEL_OBJECT_FILE);
		GZipProbTopicModelingAlgorithmStateWriter writer = new GZipProbTopicModelingAlgorithmStateWriter();
		writer.writeProbTopicModelState((ProbTopicModelingAlgorithmStateSupplier) algorithm, mFile);

		// print information about the model created by algorithm
		printModelInformation(corpus, algorithm);

	}

	/**
	 * Print Information about the created model.
	 * 
	 * @param corpus
	 *            corpus
	 * @param algorithm
	 *            algorithm
	 */
	public void printModelInformation(Corpus corpus, ModelingAlgorithm algorithm) {
		ProbTopicModelingAlgorithmStateSupplier probTopicModeling = (ProbTopicModelingAlgorithmStateSupplier) algorithm;
		LDAModel ldaModel = (LDAModel) algorithm.getModel();
		Vocabulary vocabulary = probTopicModeling.getVocabulary();

		LOGGER.info("\n\nInformation about generated Model:\n");
		LOGGER.info("NumberOfDocuments: " + probTopicModeling.getNumberOfDocuments());
		LOGGER.info("NumberOfTopics: " + probTopicModeling.getNumberOfTopics());
		LOGGER.info("NumberOfWords: " + probTopicModeling.getNumberOfWords());
		LOGGER.info("\n");

		for (int i = 0; i < probTopicModeling.getNumberOfWords(); i++)
			LOGGER.info("WordID[" + i + "] = " + vocabulary.getWord(i));
		LOGGER.info("\n");

		LOGGER.info("Information about Documents:\n");
		for (int i = 0; i < probTopicModeling.getNumberOfDocuments(); i++)
			LOGGER.info(corpus.getDocument(i).toString());

		LOGGER.info("\n\nTopic distributions for documents:");
		for (int i = 0; i < probTopicModeling.getNumberOfDocuments(); i++) {
			DocumentWordCounts wordCounts = corpus.getDocument(i).getProperty(DocumentWordCounts.class);
			double[] topicProbabilitiesForDocument = ldaModel.getTopicProbabilitiesForDocument(wordCounts);
			String vector = "Topic distributions for " + i + ".document: (";
			for (int j = 0; j < probTopicModeling.getNumberOfTopics(); j++) {
				vector += (j == probTopicModeling.getNumberOfTopics() - 1) ? topicProbabilitiesForDocument[j] + ")"
						: topicProbabilitiesForDocument[j] + ", ";
			}
			LOGGER.info(vector);
		}

		LOGGER.info("\n\nTopics:");
		ArrayList<Set<Integer>> topicContainsWords = new ArrayList<>();
		for (int i = 0; i < probTopicModeling.getNumberOfTopics(); i++)
			topicContainsWords.add(new HashSet<Integer>());
		for (int i = 0; i < probTopicModeling.getNumberOfDocuments(); i++) {
			Document document = corpus.getDocument(i);
			int[] tempTopicAssignments = ldaModel.inferTopicAssignmentsForDocument(document);
			for (int j = 0; j < tempTopicAssignments.length; j++) {
				topicContainsWords.get(tempTopicAssignments[j])
						.add(document.getProperty(DocumentTextWordIds.class).getWordIds()[j]);
			}
		}

		for (int i = 0; i < topicContainsWords.size(); i++) {
			String topics = "Topic " + i + ": ";
			for (int wordID : topicContainsWords.get(i)) {
				topics += vocabulary.getWord(wordID) + " ";

				// System.out.print(vocabulary.getWord(wordID) + "(" +
				// ldaModel.getProbabilityOfWord(wordID, i) + ") ");
			}
			LOGGER.info(topics);
		}
	}
}
