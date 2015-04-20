package org.aksw.simba.tapioca.gen;

import java.io.File;

import org.aksw.simba.topicmodeling.algorithm.mallet.MalletLdaWrapper;
import org.aksw.simba.topicmodeling.algorithms.ModelingAlgorithm;
import org.aksw.simba.topicmodeling.algorithms.ProbTopicModelingAlgorithmStateSupplier;
import org.aksw.simba.topicmodeling.io.CorpusObjectReader;
import org.aksw.simba.topicmodeling.io.CorpusReader;
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
        CorpusReader reader = new CorpusObjectReader(new File(corpusFile));
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
