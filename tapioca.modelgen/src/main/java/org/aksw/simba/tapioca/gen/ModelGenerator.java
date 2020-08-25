/**
 * tapioca.modelgen - ${project.description}
 * Copyright © 2015 Data Science Group (DICE) (michael.roeder@uni-paderborn.de)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * This file is part of tapioca.modelgen.
 *
 * tapioca.modelgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.modelgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.modelgen.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.gen;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.dice_research.topicmodeling.algorithm.mallet.MalletLdaWrapper;
import org.dice_research.topicmodeling.algorithms.ModelingAlgorithm;
import org.dice_research.topicmodeling.algorithms.ProbTopicModelingAlgorithmStateSupplier;
import org.dice_research.topicmodeling.io.CorpusReader;
import org.dice_research.topicmodeling.io.gzip.GZipCorpusReaderDecorator;
import org.dice_research.topicmodeling.io.gzip.GZipProbTopicModelingAlgorithmStateWriter;
import org.dice_research.topicmodeling.io.java.CorpusObjectReader;
import org.dice_research.topicmodeling.utils.corpus.Corpus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ModelGenerator.class);

    private static final int DEFAULT_NUMBER_OF_STEPS = 1040;
    private static final String DEFAULT_MODEL_OBJECT_FILE_NAME = "probAlgState.object";

    public static void main(String[] args) {
        // create CLI Options object
        Options options = new Options();
        options.addOption("t", "topics", true, "the number of topics");
        options.addOption("i", "iterations", true, "the number of iterations used for generating the model");
        options.addOption("o", "output-folder", true, "the folder to which the output will be written to");
        options.addOption("c", "corpus", true, "the corpus file");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            LOGGER.error("Couldn't parse commands. Aborting.", e);
            return;
        }
        if (!cmd.hasOption("c")) {
            LOGGER.error("The input corpus file is not defined. Please provide a corpus file.");
            return;
        }
        String inputFile = cmd.getOptionValue("c");
        if (!cmd.hasOption("o")) {
            LOGGER.error("Output file is not defined. Please provide an output directory.");
            return;
        }
        File outputFolder = new File(cmd.getOptionValue("o"));
        if (!cmd.hasOption("t")) {
            LOGGER.error("The number of topics is not defined. Please provide it.");
            return;
        }
        int numberOfTopics = Integer.parseInt(cmd.getOptionValue("t"));
        int iterations = DEFAULT_NUMBER_OF_STEPS;
        if (!cmd.hasOption("i")) {
            LOGGER.info("The number of iterations is not defined. The default value {} will be used", iterations);
        } else {
            iterations = Integer.parseInt(cmd.getOptionValue("i"));
        }
        
        if (!outputFolder.exists()) {
            outputFolder.mkdirs();
        }
        String modelObjFile = outputFolder.getAbsolutePath() + File.separator + DEFAULT_MODEL_OBJECT_FILE_NAME;
        
        ModelGenerator generator = new ModelGenerator(numberOfTopics, iterations);
        generator.run(inputFile, modelObjFile);
    }

    private int numberOfTopics;
    private int numberOfSteps;

    public ModelGenerator(int numberOfTopics, int numberOfSteps) {
        this.numberOfTopics = numberOfTopics;
        this.numberOfSteps = numberOfSteps;
    }

    public void run(String corpusFile, String modelFile) {
        CorpusReader reader = new GZipCorpusReaderDecorator(new CorpusObjectReader());
        reader.readCorpus(new File(corpusFile));
        Corpus corpus = reader.getCorpus();
        if (corpus == null) {
            LOGGER.error("Couldn't load corpus from file. Aborting.");
            return;
        }
        ModelingAlgorithm algorithm = new MalletLdaWrapper(numberOfTopics);
        algorithm.initialize(corpus);
        for (int i = 0; i < numberOfSteps; ++i) {
            algorithm.performNextStep();
        }
        File mFile = new File(modelFile);
        GZipProbTopicModelingAlgorithmStateWriter writer = new GZipProbTopicModelingAlgorithmStateWriter();
        writer.writeProbTopicModelState((ProbTopicModelingAlgorithmStateSupplier) algorithm, mFile);
    }
}
