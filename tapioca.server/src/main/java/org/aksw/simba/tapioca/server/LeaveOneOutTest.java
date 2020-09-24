package org.aksw.simba.tapioca.server;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.aksw.simba.tapioca.gen.ModelGenerator;
import org.aksw.simba.tapioca.gen.URIBasedIndexGenerator;
import org.aksw.simba.tapioca.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.WordOccurence;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.dice_research.topicmodeling.commons.collections.TopDoubleObjectCollection;
import org.dice_research.topicmodeling.io.CorpusReader;
import org.dice_research.topicmodeling.io.gzip.GZipCorpusReaderDecorator;
import org.dice_research.topicmodeling.io.java.CorpusObjectReader;
import org.dice_research.topicmodeling.preprocessing.ListCorpusCreator;
import org.dice_research.topicmodeling.preprocessing.Preprocessor;
import org.dice_research.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.AbstractDocumentSupplierDecorator;
import org.dice_research.topicmodeling.utils.corpus.Corpus;
import org.dice_research.topicmodeling.utils.corpus.DocumentListCorpus;
import org.dice_research.topicmodeling.utils.doc.Document;
import org.dice_research.topicmodeling.utils.doc.DocumentName;
import org.dice_research.topicmodeling.utils.doc.DocumentURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A main class that is used to execute leave one out tests with one of the
 * available search engines.
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class LeaveOneOutTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaveOneOutTest.class);

    private static int RUNS = 5;
    private static int NUMBER_OF_TOPICS = 25;
    private static WordOccurence WORD_OCCURENCE = WordOccurence.UNIQUE;
    private static UriUsage URI_USAGE = UriUsage.PROPERTIES;
    private static Algorithm ALGORITHM = Algorithm.TAPIOCA;
    private static boolean USE_FILE_NAMES = true;

    public enum Algorithm {
        BL, TFIDF, TAPIOCA;
    }

    public static void main(String[] args) {
        // define paths
        StringBuilder corpusPathBuilder = new StringBuilder();
        StringBuilder outputPathBuilder = new StringBuilder();
        corpusPathBuilder.append("/home/micha/data/sven/sven-corpora/lda-corpus-");
        outputPathBuilder.append("/home/micha/data/sven/leave-one-out-output/");
        switch (URI_USAGE) {
        case CLASSES: {
            corpusPathBuilder.append('c');
            outputPathBuilder.append('c');
            break;
        }
        case CLASSES_AND_PROPERTIES: {
            corpusPathBuilder.append('a');
            outputPathBuilder.append('a');
            break;
        }
        case PROPERTIES: {
            corpusPathBuilder.append('p');
            outputPathBuilder.append('p');
            break;
        }
        default:
            LOGGER.error(URI_USAGE + " is not supported!");
            break;
        }
        corpusPathBuilder.append('-');
        switch (WORD_OCCURENCE) {
        case LOG: {
            corpusPathBuilder.append('l');
            break;
        }
        case UNIQUE: {
            corpusPathBuilder.append('u');
            break;
        }
        default:
            LOGGER.error(WORD_OCCURENCE + " is not supported!");
            break;
        }
        corpusPathBuilder.append(".object");

        Corpus corpus = null;
        EngineFactory factory = null;
        int runs = 1;
        switch (ALGORITHM) {
        case BL: {
            corpus = readBLCorpus("/home/micha/data/sven/sven-corpora/initial.xml");
            factory = new BLFactory();
            break;
        }
        case TFIDF: {
            corpus = readTMCorpus(corpusPathBuilder.toString());
            factory = new TfidfFactory();
            break;
        }
        case TAPIOCA: {
            // create the corpus name
            corpus = readTMCorpus(corpusPathBuilder.toString());
            outputPathBuilder.append(File.separator);
            switch (WORD_OCCURENCE) {
            case LOG: {
                outputPathBuilder.append('l');
                break;
            }
            case UNIQUE: {
                outputPathBuilder.append('u');
                break;
            }
            default:
                LOGGER.error(WORD_OCCURENCE + " is not supported!");
                break;
            }
            outputPathBuilder.append(File.separator);
            outputPathBuilder.append(NUMBER_OF_TOPICS);
            outputPathBuilder.append("-topics");
            factory = new TMFactory(NUMBER_OF_TOPICS);
            runs = RUNS;
            break;
        }
        default: {
            LOGGER.error("Unknown algorithm!");
            return;
        }
        }

        File outputDirectory = new File(outputPathBuilder.toString());
        outputDirectory.mkdirs();

        File runOutput;
        for (int i = 0; i < runs; ++i) {
            LOGGER.info("Run {} starts...", i);
            if (runs > 1) {
                runOutput = new File(outputDirectory.getAbsolutePath() + File.separator + Integer.toString(i));
                runOutput.mkdir();
            } else {
                runOutput = outputDirectory;
            }
            leaveOneOut(corpus, factory, runOutput);
        }
    }

    public static void leaveOneOut(Corpus corpus, EngineFactory factory, File outputDirectory) {
        List<Document> trainDocuments;
        Corpus trainCorpus;
        Document testDocument;
        for (int i = 0; i < corpus.getNumberOfDocuments(); ++i) {
            LOGGER.info("Starting test with document {} as held-out document.", i);
            if (i == 0) {
                trainDocuments = new ArrayList<>();
            } else {
                trainDocuments = new ArrayList<>(corpus.getDocuments(0, i));
            }
            if (i < (corpus.getNumberOfDocuments() - 1)) {
                trainDocuments.addAll(corpus.getDocuments(i + 1, corpus.getNumberOfDocuments()));
            }
            trainCorpus = new DocumentListCorpus<List<Document>>(trainDocuments);
            trainCorpus.setProperties(corpus.getProperties());

            testDocument = corpus.getDocument(i);

            File foldDirectory = new File(outputDirectory.getAbsolutePath() + File.separator + Integer.toString(i));
            AbstractEngine engine = factory.create(trainCorpus, foldDirectory);
            TopDoubleObjectCollection<String> result = engine.retrieveSimilarDatasets(testDocument);

            printResult(result, testDocument, factory.getEngineDesc(), outputDirectory);

            trainDocuments = null;
            trainCorpus = null;
            engine = null;
            result = null;
            System.gc();
        }
    }

    private static void printResult(TopDoubleObjectCollection<String> result, Document testDocument, String engineDesc,
            File foldDirectory) {
        String name;
        DocumentName dName = testDocument.getProperty(DocumentName.class);
        if (dName == null) {
            DocumentURI dUri = testDocument.getProperty(DocumentURI.class);
            if (dUri == null) {
                name = Integer.toString(testDocument.getDocumentId());
            } else {
                name = dUri.get();
            }
        } else {
            name = dName.get();
        }
        File outputFile = new File(
                foldDirectory.getAbsolutePath() + File.separator + engineDesc + File.separator + name + ".csv");
        outputFile.getParentFile().mkdir();
        try (PrintStream pout = new PrintStream(new BufferedOutputStream(new FileOutputStream(outputFile, true)))) {
            pout.println("File,Score");
            for (int i = 0; i < result.size(); ++i) {
                pout.print(result.objects[i]);
                pout.print(',');
                pout.print(result.values[i]);
                pout.println();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    protected static Corpus readTMCorpus(String inputFile) {
        CorpusReader reader = new GZipCorpusReaderDecorator(new CorpusObjectReader());
        reader.readCorpus(new File(inputFile));
        Corpus corpus = reader.getCorpus();
        if (USE_FILE_NAMES) {
            ReplacingDecorator replacer = new ReplacingDecorator(null);
            for (Document document : corpus) {
                replacer.apply(document);
            }
        }
        return corpus;
    }

    protected static Corpus readBLCorpus(String inputFile) {
        DocumentSupplier supplier = URIBasedIndexGenerator.createBLPreprocessing(new File(inputFile));
        if (USE_FILE_NAMES) {
            supplier = new ReplacingDecorator(supplier);
        }
        Preprocessor preprocessor = new ListCorpusCreator<>(supplier,
                new DocumentListCorpus<List<Document>>(new ArrayList<>()));

        return preprocessor.getCorpus();
    }

    public static interface EngineFactory {
        public AbstractEngine create(Corpus trainCorpus, File outputDirectory);

        public String getEngineDesc();
    }

    public static class BLFactory implements EngineFactory {
        @Override
        public AbstractEngine create(Corpus trainCorpus, File outputDirectory) {
            BLEngine engine = BLEngine.createEngine(trainCorpus, null, URI_USAGE);
            engine.setNumberOfResults(trainCorpus.getNumberOfDocuments());
            return engine;
        }

        @Override
        public String getEngineDesc() {
            return "BL";
        }
    }

    public static class TMFactory implements EngineFactory {
        private int numberOfTopics = 25;
        private WorkerBasedLabelRetrievingDocumentSupplierDecorator decorator = new WorkerBasedLabelRetrievingDocumentSupplierDecorator(
                null, new File[0]);

        public TMFactory(int numberOfTopics) {
            this.numberOfTopics = numberOfTopics;
        }

        @Override
        public AbstractEngine create(Corpus trainCorpus, File outputDirectory) {
            if (!outputDirectory.exists()) {
                outputDirectory.mkdir();
            }
            // Generate model
            String modelObjFile = outputDirectory.getAbsolutePath() + File.separator + TMEngine.MODEL_FILE_NAME;
            ModelGenerator generator = new ModelGenerator(numberOfTopics, 1040);
            generator.run(trainCorpus, modelObjFile);
            // Create TM engine
            TMEngine engine = TMEngine.createEngine(decorator, trainCorpus, outputDirectory, null, URI_USAGE,
                    WORD_OCCURENCE);
            engine.setNumberOfResults(trainCorpus.getNumberOfDocuments());
            return engine;
        }

        @Override
        public String getEngineDesc() {
            return "Tapioca";
        }
    }

    public static class TfidfFactory implements EngineFactory {

        public TfidfFactory() {
        }

        @Override
        public AbstractEngine create(Corpus trainCorpus, File outputDirectory) {
            TfidfEngine engine = TfidfEngine.createEngine(trainCorpus, null, URI_USAGE);
            engine.setNumberOfResults(trainCorpus.getNumberOfDocuments());
            return engine;
        }

        @Override
        public String getEngineDesc() {
            return "TFIDF";
        }
    }

    public static class ReplacingDecorator extends AbstractDocumentSupplierDecorator {

        public ReplacingDecorator(DocumentSupplier documentSource) {
            super(documentSource);
        }

        @Override
        protected Document prepareDocument(Document document) {
            DocumentName dName = document.getProperty(DocumentName.class);
            document.addProperty(new DocumentURI(dName.getStringValue()));
            return document;
        }

    }
}
