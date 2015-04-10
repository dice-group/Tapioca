package org.aksw.simba.tapioca.gen;

import java.io.File;

import org.aksw.simba.topicmodeling.algorithm.mallet.MalletLdaWrapper;
import org.aksw.simba.topicmodeling.algorithms.ModelingAlgorithm;
import org.aksw.simba.topicmodeling.io.CorpusObjectReader;
import org.aksw.simba.topicmodeling.io.CorpusReader;
import org.aksw.simba.topicmodeling.io.ModelWriter;
import org.aksw.simba.topicmodeling.io.gzip.GZipModelObjectWriter;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ModelGenerator.class);

    private static final int NUMBER_OF_TOPICS = 1000;
    private static final int NUMBER_OF_STEPS = 1040;
    private static final String MODEL_FOLDER = "/home/mroeder/tapioca/model";
    private static final String MODEL_OBJECT_FILE = MODEL_FOLDER + File.separator + "model.object";
    private static final String CORPUS_FILE = "/home/mroeder/tapioca/lodStats_all_log.object";

    public static void main(String[] args) {
        CorpusReader reader = new CorpusObjectReader(new File(CORPUS_FILE));
        Corpus corpus = reader.getCorpus();
        if (corpus == null) {
            LOGGER.error("Couldn't load corpus from file. Aborting.");
            return;
        }
        ModelingAlgorithm algorithm = new MalletLdaWrapper(NUMBER_OF_TOPICS);
        algorithm.initialize(corpus);
        for (int i = 0; i < NUMBER_OF_STEPS; ++i) {
            algorithm.performNextStep();
        }

        File outputFolder = new File(MODEL_FOLDER);
        if (!outputFolder.exists()) {
            outputFolder.mkdirs();
        }
        File modelFile = new File(MODEL_OBJECT_FILE);
        ModelWriter writer = new GZipModelObjectWriter(modelFile);
        writer.writeModelToFiles(algorithm.getModel());
    }
}
