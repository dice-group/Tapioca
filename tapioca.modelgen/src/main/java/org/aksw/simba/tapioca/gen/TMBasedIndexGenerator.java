package org.aksw.simba.tapioca.gen;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TMBasedIndexGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TMBasedIndexGenerator.class);

    public static final String CORPUS_NAME = "lodStats";
    public static final String CORPUS_FILE = "/Daten/tapioca/" + CORPUS_NAME + ".corpus";
    public static final String LDA_CORPUS_FILE = "/Daten/tapioca/" + CORPUS_NAME + "_all_log.object";

    public static final File INPUT_FOLDER = new File("C:/Daten/Dropbox/lodstats-rdf/23032015/void");
    public static final String META_DATA_FILE = "C:/Daten/Dropbox/lodstats-rdf/23032015/datasets.nt";

    public static final String OUTPUT_FOLDER = "/Daten/tapioca/" + CORPUS_NAME + "_model";
    public static final String FINAL_CORPUS_FILE = CORPUS_NAME + "_final.corpus";
    public static final String MODEL_FILE = "probAlgState.object";

    public static void main(String[] args) {

        File outputFolder = new File(OUTPUT_FOLDER);
        if (!outputFolder.exists()) {
            outputFolder.mkdirs();
        }

        File datasetDescriptionsFile = new File(OUTPUT_FOLDER + File.separator + FINAL_CORPUS_FILE);
        if (datasetDescriptionsFile.exists()) {
            LOGGER.info("The final corpus file is already existing.");
        } else {
            generateFinalCorpusFile();
        }

//        File modelFile = new File(OUTPUT_FOLDER + File.separator + MODEL_FILE);
//        if (modelFile.exists()) {
//            LOGGER.info("The model file is already existing.");
//        } else {
//            generateModel();
//        }
    }

    private static void generateFinalCorpusFile() {
        checkLDACorpusExistence();
        MetaDataInformationCollector collector = new MetaDataInformationCollector();
        LOGGER.info("Generating final corpus file...");
        collector.run(META_DATA_FILE, LDA_CORPUS_FILE, OUTPUT_FOLDER + File.separator + FINAL_CORPUS_FILE);
    }

    private static void generateModel() {
        checkLDACorpusExistence();
        ModelGenerator generator = new ModelGenerator(1000, 1040);
        LOGGER.info("Generating Model file...");
        generator.run(LDA_CORPUS_FILE, OUTPUT_FOLDER + File.separator + MODEL_FILE);
    }

    private static void checkLDACorpusExistence() {
        File ldaCorpusFile = new File(LDA_CORPUS_FILE);
        if (!ldaCorpusFile.exists()) {
            LOGGER.warn("The LDA corpus file is not existing. Trying to generate it...");
            // TODO generate LDA corpus file
        }
    }
}
