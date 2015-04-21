package org.aksw.simba.tapioca.gen;

import java.io.File;

import org.aksw.simba.tapioca.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.WordOccurence;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TMBasedIndexGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TMBasedIndexGenerator.class);

    public static final String TAPIOCA_FOLDER = "/home/mroeder/tapioca/";

    public static final String CORPUS_NAME = "lodStats";
    public static final String CORPUS_FILE = TAPIOCA_FOLDER + CORPUS_NAME + ".corpus";
    public static final String LDA_CORPUS_FILE = TAPIOCA_FOLDER + CORPUS_NAME + "_all_log.object";
    public static final File CACHE_FILES[] = new File[] { new File(TAPIOCA_FOLDER + "cache/uriToLabelCache_1.object"),
            new File(TAPIOCA_FOLDER + "cache/uriToLabelCache_2.object"),
            new File(TAPIOCA_FOLDER + "cache/uriToLabelCache_3.object") };

    public static final File INPUT_FOLDER = new File("C:/Daten/Dropbox/lodstats-rdf/23032015/void");
    public static final String META_DATA_FILE = TAPIOCA_FOLDER + "lodStats/datasets.nt";
    public static final String STAT_RESULT_FILE = TAPIOCA_FOLDER + "lodStats/statresult.nt";

    public static final String OUTPUT_FOLDER = TAPIOCA_FOLDER + CORPUS_NAME + "_model";
    public static final String FINAL_CORPUS_FILE = CORPUS_NAME + "_final.corpus";
    public static final String MODEL_FILE = "probAlgState.object";

    public static void main(String[] args) {
        TMBasedIndexGenerator generator = new TMBasedIndexGenerator();
        generator.run();
    }

    public void run() {
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

        File modelFile = new File(OUTPUT_FOLDER + File.separator + MODEL_FILE);
        if (modelFile.exists()) {
            LOGGER.info("The model file is already existing.");
        } else {
            generateModel();
        }
    }

    protected static void generateFirstCorpusFile() {
        InitialCorpusCreation creation = new InitialCorpusCreation();
        creation.run(CORPUS_FILE, INPUT_FOLDER);
    }

    protected void generateLDACorpusFile() {
        File corpusFile = new File(CORPUS_FILE);
        if (!corpusFile.exists()) {
            LOGGER.info("The first corpus file is not existing. Trying to generate it...");
            generateFirstCorpusFile();
            if (!corpusFile.exists()) {
                LOGGER.error("The first corpus file is not existing and couldn't be generated.");
                return;
            }
        }
        LDACorpusCreation creation = new LDACorpusCreation(CORPUS_NAME, CORPUS_FILE, UriUsage.CLASSES_AND_PROPERTIES,
                WordOccurence.LOG);
        WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever;
        cachingLabelRetriever = new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, CACHE_FILES, new File[0]);
        creation.run(cachingLabelRetriever);
        cachingLabelRetriever.close();
    }

    protected void generateFinalCorpusFile() {
        if (checkLDACorpusExistence()) {
            MetaDataInformationCollector collector = new MetaDataInformationCollector();
            LOGGER.info("Generating final corpus file...");
            collector.run(META_DATA_FILE, LDA_CORPUS_FILE, STAT_RESULT_FILE, OUTPUT_FOLDER + File.separator
                    + FINAL_CORPUS_FILE);
        }
    }

    protected void generateModel() {
        if (checkLDACorpusExistence()) {
            ModelGenerator generator = new ModelGenerator(1000, 1040);
            LOGGER.info("Generating Model file...");
            generator.run(OUTPUT_FOLDER + File.separator + FINAL_CORPUS_FILE, OUTPUT_FOLDER + File.separator
                    + MODEL_FILE);
        }
    }

    protected boolean checkLDACorpusExistence() {
        File ldaCorpusFile = new File(LDA_CORPUS_FILE);
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
