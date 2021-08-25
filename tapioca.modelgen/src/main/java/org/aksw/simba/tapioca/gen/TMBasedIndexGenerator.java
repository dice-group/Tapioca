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
import java.io.IOException;

import org.aksw.simba.tapioca.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.WordOccurence;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TMBasedIndexGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TMBasedIndexGenerator.class);

//    public static final String TAPIOCA_FOLDER = "/home/mroeder/tapioca/";
    public static final String TAPIOCA_FOLDER = "/Daten/tapioca/";

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
    public static final String MODEL_META_DATA_FILE = "lodstats.nt";

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
        creation.run(INPUT_FOLDER, new File(CORPUS_FILE));
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
        // FIXME fix this by using the output file instead of the corpus name
//        LDACorpusCreation creation = new LDACorpusCreation(CORPUS_NAME, CORPUS_FILE, UriUsage.CLASSES_AND_PROPERTIES,
//                WordOccurence.LOG);
        LDACorpusCreation creation = new LDACorpusCreation(CORPUS_FILE, UriUsage.CLASSES_AND_PROPERTIES,
                WordOccurence.LOG, null);
        WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever;
        cachingLabelRetriever = new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, CACHE_FILES, new File[0]);
        try {
            creation.run(cachingLabelRetriever);
        } catch (IOException e) {
            LOGGER.error("Exception while generating LDA corpus.", e);
        } finally {
            cachingLabelRetriever.close();
        }
    }

    protected void generateFinalCorpusFile() {
        if (checkLDACorpusExistence()) {
            MetaDataInformationCollector collector = new MetaDataInformationCollector();
            LOGGER.info("Generating final corpus file...");
            collector.run(META_DATA_FILE, LDA_CORPUS_FILE, STAT_RESULT_FILE,
                    OUTPUT_FOLDER + File.separator + FINAL_CORPUS_FILE,
                    OUTPUT_FOLDER + File.separator + MODEL_META_DATA_FILE);
        }
    }

    protected void generateModel() {
        if (checkLDACorpusExistence()) {
            ModelGenerator generator = new ModelGenerator(1000, 1040);
            LOGGER.info("Generating Model file...");
            generator.run(OUTPUT_FOLDER + File.separator + FINAL_CORPUS_FILE,
                    OUTPUT_FOLDER + File.separator + MODEL_FILE);
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
