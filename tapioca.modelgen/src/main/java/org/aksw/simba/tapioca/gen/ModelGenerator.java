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

import org.aksw.simba.topicmodeling.algorithm.mallet.MalletLdaWrapper;
import org.aksw.simba.topicmodeling.algorithms.ModelingAlgorithm;
import org.aksw.simba.topicmodeling.algorithms.ProbTopicModelingAlgorithmStateSupplier;
import org.aksw.simba.topicmodeling.io.CorpusReader;
import org.aksw.simba.topicmodeling.io.gzip.GZipCorpusObjectReader;
import org.aksw.simba.topicmodeling.io.gzip.GZipProbTopicModelingAlgorithmStateWriter;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ModelGenerator.class);

    private static final int NUMBER_OF_TOPICS = 1000;
    private static final int NUMBER_OF_STEPS = 1040;
    private static final String MODEL_FOLDER = "/home/mroeder/tapioca/model";
    private static final String MODEL_OBJECT_FILE = MODEL_FOLDER + File.separator + "probAlgState.object";
    private static final String CORPUS_FILE = "/home/mroeder/tapioca/lodStats_all_log.object";

    public static void main(String[] args) {
        File outputFolder = new File(MODEL_FOLDER);
        if (!outputFolder.exists()) {
            outputFolder.mkdirs();
        }
        ModelGenerator generator = new ModelGenerator();
        generator.run(CORPUS_FILE, MODEL_OBJECT_FILE);
    }

    private int numberOfTopics;
    private int numberOfSteps;

    public ModelGenerator() {
        this(NUMBER_OF_TOPICS, NUMBER_OF_STEPS);
    }

    public ModelGenerator(int numberOfTopics, int numberOfSteps) {
        this.numberOfTopics = numberOfTopics;
        this.numberOfSteps = numberOfSteps;
    }

    public void run(String corpusFile, String modelFile) {
        CorpusReader reader = new GZipCorpusObjectReader(new File(corpusFile));
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
