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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.data.DatasetSpecialClassesInfo;
import org.aksw.simba.tapioca.data.DatasetVocabularies;
import org.aksw.simba.tapioca.data.SimpleTokenizedText;
import org.aksw.simba.tapioca.data.StringCountMapping;
import org.aksw.simba.tapioca.data.VocabularyBlacklist;
import org.aksw.simba.tapioca.preprocessing.SimpleBlankNodeRemovingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.SimpleTokenizedTextTermFilter;
import org.aksw.simba.tapioca.preprocessing.SimpleWordIndexingSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.WordOccurence;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.tapioca.preprocessing.UriFilteringDocumentSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.FileBasedTokenizedLabelRetriever;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.LODCatLabelServiceBasedRetriever;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.MongoDBBasedTokenizedLabelRetriever;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.TokenizedLabelRetriever;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.dice_research.topicmodeling.io.CorpusWriter;
import org.dice_research.topicmodeling.io.gzip.GZipCorpusWriterDecorator;
import org.dice_research.topicmodeling.io.java.CorpusObjectWriter;
import org.dice_research.topicmodeling.io.xml.XmlWritingDocumentConsumer;
import org.dice_research.topicmodeling.io.xml.stream.StreamBasedXmlDocumentSupplier;
import org.dice_research.topicmodeling.lang.postagging.StandardEnglishPosTaggingTermFilter;
import org.dice_research.topicmodeling.preprocessing.ListCorpusCreator;
import org.dice_research.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.DocumentConsumerAdaptingSupplierDecorator;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.DocumentFilteringSupplierDecorator;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.DocumentWordCountingSupplierDecorator;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.PropertyRemovingSupplierDecorator;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.filter.DocumentFilter;
import org.dice_research.topicmodeling.utils.corpus.Corpus;
import org.dice_research.topicmodeling.utils.corpus.DocumentListCorpus;
import org.dice_research.topicmodeling.utils.corpus.properties.CorpusVocabulary;
import org.dice_research.topicmodeling.utils.doc.Document;
import org.dice_research.topicmodeling.utils.doc.DocumentName;
import org.dice_research.topicmodeling.utils.doc.DocumentProperty;
import org.dice_research.topicmodeling.utils.doc.DocumentURI;
import org.dice_research.topicmodeling.utils.vocabulary.SimpleVocabulary;
import org.dice_research.topicmodeling.utils.vocabulary.Vocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LDACorpusCreation {

    private static final Logger LOGGER = LoggerFactory.getLogger(LDACorpusCreation.class);

    @Deprecated
    public static final File CACHE_FILES[] = new File[] { new File("C:/Daten/tapioca/cache/uriToLabelCache_1.object"),
            new File("C:/Daten/tapioca/cache/uriToLabelCache_2.object"),
            new File("C:/Daten/tapioca/cache/uriToLabelCache_3.object") };
    // public static final File CACHE_FILES[] = new File[] { new
    // File("/home/mroeder/tapioca/uriToLabelCache_1.object"),
    // new File("/home/mroeder/tapioca/uriToLabelCache_2.object"),
    // new File("/home/mroeder/tapioca/uriToLabelCache_3.object") };

    // public static final String CORPUS_NAME = "lodStatsGold";
    @Deprecated
    public static final String CORPUS_NAME = "lodDiagram";
    @Deprecated
    public static final String CORPUS_FILE = "/Daten/tapioca/" + CORPUS_NAME + ".corpus";
//    @Deprecated
//    private static final boolean EXPORT_CORPUS_AS_XML = false;

    public static void main(String[] args) throws IOException {
        // create CLI Options object
        Options options = new Options();
        options.addOption("c", "cache-file", true,
                "a cache file that can be used to cache labels retrieved via HTTP. Only used in combination with -y.");
        options.addOption("f", "word-frequency", true, "either \"u\" for unique or \"l\" for log. \"l\" is default.");
        options.addOption("h", "mongo-db-host", true,
                "the host name of a MongoDB instance containing URI to label mappings");
        options.addOption("l", "label-file", true, "a label file that should be used to retrieve labels");
        options.addOption("n", "input-file", true, "the input corpus file");
        options.addOption("o", "output-file", true, "the output corpus file");
        options.addOption("p", "mongo-db-port", true,
                "the port of a MongoDB instance containing URI to label mappings");
        options.addOption("s", "label-service", true, "the URL of a label retrieval service");
        options.addOption("u", "uri-type", true,
                "either \"c\" for classes, \"p\" for properties or \"a\" for all. \"p\" is default.");
        options.addOption("w", "workers", true, "number of workers used for retrieving labels");
        options.addOption("x", "export-xml", false, "export the corpus as XML");
        options.addOption("y", "http-client", false,
                "if set, labels for URIs are retrieved via HTTP. Note that this make take a lot of time!");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            LOGGER.error("Couldn't parse commands. Aborting.", e);
            return;
        }
        if (!cmd.hasOption("n")) {
            LOGGER.error("Input file is not defined. Please provide an input file.");
            return;
        }
        String inputFile = cmd.getOptionValue("n");
        if (!cmd.hasOption("o")) {
            LOGGER.error("Output file is not defined. Please provide an Output file.");
            return;
        }
        String outputFile = cmd.getOptionValue("o");

        WordOccurence wordOccurence = null;
        if (cmd.hasOption("f")) {
            String value = cmd.getOptionValue("f");
            switch (value) {
            case "l": {
                wordOccurence = WordOccurence.LOG;
                break;
            }
            case "u": {
                wordOccurence = WordOccurence.UNIQUE;
                break;
            }
            default: {
                LOGGER.error("Got an unkown value for the uri-type: \"" + value + "\"");
                return;
            }
            }
        } else {
            wordOccurence = WordOccurence.UNIQUE;
        }

        UriUsage uriUsage = null;
        if (cmd.hasOption("u")) {
            String value = cmd.getOptionValue("u");
            switch (value) {
            case "a": {
                uriUsage = UriUsage.CLASSES_AND_PROPERTIES;
                break;
            }
            case "c": {
                uriUsage = UriUsage.CLASSES;
                break;
            }
            case "p": {
                uriUsage = UriUsage.PROPERTIES;
                break;
            }
            default: {
                LOGGER.error("Got an unkown value for the uri-type: \"" + value + "\"");
                return;
            }
            }
        } else {
            uriUsage = UriUsage.PROPERTIES;
        }
        // UriUsage uriUsages[] = new UriUsage[] { UriUsage.CLASSES_AND_PROPERTIES };
        // WordOccurence wordOccurences[] = new WordOccurence[] { /*
        // WordOccurence.UNIQUE, */WordOccurence.LOG };

//        String corpusName = CORPUS_NAME;

        // File labelsFiles[] = new File[] { new File(CORPUS_FILE.replace(".corpus",
        // ".labels.object")),
        // new File(CORPUS_FILE.replace(".corpus", ".ret_labels_1.object")) };

        MongoDBBasedTokenizedLabelRetriever mongoRetriever = null;
        WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever = null;
        try {
            List<TokenizedLabelRetriever> retrievers = new ArrayList<>();
            if (cmd.hasOption("h") || cmd.hasOption("p")) {
                if (cmd.hasOption("h") && cmd.hasOption("p")) {
                    mongoRetriever = MongoDBBasedTokenizedLabelRetriever.create(cmd.getOptionValue("h"),
                            Integer.parseInt(cmd.getOptionValue("p")));
                    retrievers.add(mongoRetriever);
                } else {
                    LOGGER.error(
                            "If one of the options h or p is defined, the other option has to be defined as well.");
                    return;
                }
            }
            if (cmd.hasOption("l")) {
                for (String file : cmd.getOptionValues("l")) {
                    initFileBasedRetriever(retrievers, file);
                }
            }
            if (cmd.hasOption("s")) {
                retrievers.add(new LODCatLabelServiceBasedRetriever(cmd.getOptionValue("s")));
            }
            boolean useHttpClient = cmd.hasOption('y');
            File cacheFiles[] = null;
            if (cmd.hasOption("c")) {
                String fileNames[] = cmd.getOptionValues("c");
                cacheFiles = new File[fileNames.length];
                for (int i = 0; i < fileNames.length; ++i) {
                    cacheFiles[i] = new File(fileNames[i]);
                }
            } else {
                cacheFiles = new File[0];
            }
            int numberOfWorkers = -1;
            if (cmd.hasOption('w')) {
                try {
                    numberOfWorkers = Integer.parseInt(cmd.getOptionValue('w'));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Couldn't parse given number of workers.", e);
                }
                if (numberOfWorkers < 1) {
                    throw new IllegalArgumentException("\"" + numberOfWorkers
                            + "\" is not a valid number of workers. The numbers is expected to be >= 1.");
                }
            }
            if (numberOfWorkers > 0) {
                cachingLabelRetriever = new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, cacheFiles,
                        retrievers.stream().filter(r -> r != null).toArray(TokenizedLabelRetriever[]::new),
                        numberOfWorkers, useHttpClient);
            } else {
                cachingLabelRetriever = new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, cacheFiles,
                        retrievers.stream().filter(r -> r != null).toArray(TokenizedLabelRetriever[]::new),
                        useHttpClient);
            }
            // LabelRetrievingDocumentSupplierDecorator cachingLabelRetriever;
            // cachingLabelRetriever = new
            // LabelRetrievingDocumentSupplierDecorator(null, false, labelsFiles);

            LDACorpusCreation corpusCreation;
//            for (int i = 0; i < uriUsages.length; ++i) {
//                for (int j = 0; j < wordOccurences.length; ++j) {
//                    System.out.println(
//                            "Starting corpus \"" + inputFile + "\" with " + uriUsages[i] + " and " + wordOccurences[j]);
//                    corpusCreation = new LDACorpusCreation(inputFile, uriUsages[i], wordOccurences[j], outputFile);
            System.out.println("Starting corpus \"" + inputFile + "\" with " + uriUsage + " and " + wordOccurence);
            corpusCreation = new LDACorpusCreation(inputFile, uriUsage, wordOccurence, outputFile);
            corpusCreation.run(cachingLabelRetriever);
//                }
//            }
        } finally {
            if (mongoRetriever != null) {
                try {
                    mongoRetriever.close();
                } catch (Exception e) {
                }
            }
            if (cachingLabelRetriever != null) {
                try {
                    cachingLabelRetriever.close();
                } catch (Exception e) {
                }
            }
        }

    }

    private static void initFileBasedRetriever(List<TokenizedLabelRetriever> retrievers, String file) {
        File f = new File(file);
        if (f.isDirectory()) {
            for (File f2 : f.listFiles()) {
                initFileBasedRetriever(retrievers, f2.getAbsolutePath());
            }
        } else {
            retrievers.add(FileBasedTokenizedLabelRetriever.create(file));
        }
    }

    protected final String inputFile;
    protected final String outputFile;
    protected final UriUsage uriUsage;
    protected final WordOccurence wordOccurence;
    protected final boolean exportCorpusAsXml;

    public LDACorpusCreation(String inputFile, UriUsage uriUsage, WordOccurence wordOccurence, String outputFile) {
        this(inputFile, uriUsage, wordOccurence, outputFile, false);
    }

    public LDACorpusCreation(String inputFile, UriUsage uriUsage, WordOccurence wordOccurence, String outputFile,
            boolean exportCorpusAsXml) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.uriUsage = uriUsage;
        this.wordOccurence = wordOccurence;
        this.exportCorpusAsXml = exportCorpusAsXml;
    }

    public void run(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) throws IOException {
        // String corpusName = generateCorpusName();

        XmlWritingDocumentConsumer consumer = null;
        if (exportCorpusAsXml) {
            consumer = XmlWritingDocumentConsumer.createXmlWritingDocumentConsumer(new File("./export.xml"));
        }

        Corpus corpus = generateCorpusAndIndexWords(cachingLabelRetriever, consumer);

        cachingLabelRetriever.storeCache();
        if (consumer != null) {
            IOUtils.closeQuietly(consumer);
        }

        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(outputFile)))) {
            CorpusWriter writer = new GZipCorpusWriterDecorator(new CorpusObjectWriter());
            writer.writeCorpus(corpus, out);
        }
    }

    /**
     * Reads the corpus from the XML file created by the
     * {@link InitialCorpusCreation}.
     * 
     * @return DocumentSupplier managing the stream of documents.
     */
    protected DocumentSupplier readCorpus() {
        DocumentSupplier supplier = StreamBasedXmlDocumentSupplier.createReader(new File(inputFile), true);
        StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetClassInfo.class);
        StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetSpecialClassesInfo.class);
        StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetPropertyInfo.class);
        StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetVocabularies.class);

        supplier = new DocumentFilteringSupplierDecorator(supplier, new DocumentFilter() {
            public boolean isDocumentGood(Document document) {
                DocumentName name = document.getProperty(DocumentName.class);
                DocumentURI uri = document.getProperty(DocumentURI.class);
                LOGGER.info("Processing of {} ({}) starts", name != null ? name.get() : "null",
                        uri != null ? uri.get() : "null");
                return true;
            }
        });
        return supplier;
    }

    /**
     * Applies a white list filter if there is a white list filter file for this
     * corpus.
     * 
     * @param supplier
     * @return
     */
    protected DocumentSupplier useWhiteListFilter(DocumentSupplier supplier) {
        if (inputFile.contains(".corpus")) {
            File whitelistFile = new File(inputFile.replace(".corpus", "_whitelist.txt"));
            if (whitelistFile.exists()) {
                try {
                    final Set<String> whitelist = new HashSet<String>(FileUtils.readLines(whitelistFile));
                    supplier = new DocumentFilteringSupplierDecorator(supplier, new DocumentFilter() {
                        public boolean isDocumentGood(Document document) {
                            DocumentName docName = document.getProperty(DocumentName.class);
                            if (docName != null) {
                                String name = docName.get();
                                if (name.endsWith(".ttl")) {
                                    name = name.substring(0, name.length() - 4);
                                }
                                DocumentURI uri = document.getProperty(DocumentURI.class);
                                return whitelist.contains(name) || ((uri != null) && (whitelist.contains(uri.get())));
                            } else {
                                return false;
                            }
                        }
                    });
                    LOGGER.info("Using whitelistfile \"{}\".", whitelistFile);
                } catch (IOException e) {
                    LOGGER.error("Error while reading whitelist \"" + whitelistFile + "\".", e);
                }
            } else {
                LOGGER.info("Can't use whitelistfile \"{}\".", whitelistFile);
            }
        } else {
            LOGGER.info("No whitelistfile given");
        }
        return supplier;
    }

    /**
     * Generates tokenized documents based on their URI counts. URIs are filtered
     * and counted. After that their labels are retrieved and added to the documents
     * {@link SimpleTokenizedText} based on the {@link WordOccurence} used.
     * 
     * @param supplier
     * @param cachingLabelRetriever
     * @return
     */
    protected DocumentSupplier generateDocuments(DocumentSupplier supplier,
            WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {
        // Filter URIs
        supplier = filterUris(supplier);
        // Filter documents with missing property or class URIs
        // supplier = new DocumentFilteringSupplierDecorator(supplier, new
        // NoClassAndPropertyDocumentFilter());

        // Count the URIs
        supplier = new UriCountMappingCreatingDocumentSupplierDecorator(supplier, uriUsage);

        // Retrieve and tokenize the labels
        // LabelRetrievingDocumentSupplierDecorator cachingLabelRetriever = new
        // LabelRetrievingDocumentSupplierDecorator(
        // supplier);
        // Check whether there is a file containing labels
        cachingLabelRetriever.setDecoratedDocumentSupplier(supplier);
        supplier = cachingLabelRetriever;
        // supplier = new ExceptionCatchingDocumentSupplierDecorator(supplier);
        // Convert the counted tokens into tokenized text
        supplier = new StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator(supplier, wordOccurence);
        return supplier;
    }

    /**
     * Filters URIs based on the {@link VocabularyBlacklist}.
     * 
     * @param supplier
     * @return
     */
    protected DocumentSupplier filterUris(DocumentSupplier supplier) {
        Set<String> blacklist = VocabularyBlacklist.getInstance();
        supplier = new UriFilteringDocumentSupplierDecorator<DatasetClassInfo>(supplier, blacklist,
                DatasetClassInfo.class);
        supplier = new SimpleBlankNodeRemovingDocumentSupplierDecorator<DatasetClassInfo>(supplier,
                DatasetClassInfo.class);
        supplier = new UriFilteringDocumentSupplierDecorator<DatasetPropertyInfo>(supplier, blacklist,
                DatasetPropertyInfo.class);
        supplier = new SimpleBlankNodeRemovingDocumentSupplierDecorator<DatasetPropertyInfo>(supplier,
                DatasetPropertyInfo.class);
        supplier = new SimpleBlankNodeRemovingDocumentSupplierDecorator<DatasetSpecialClassesInfo>(supplier,
                DatasetSpecialClassesInfo.class);
        return supplier;
    }

    protected DocumentSupplier filterStopWordsAndEmptyDocs(DocumentSupplier supplier) {
        // Filter the stop words
        supplier = new SimpleTokenizedTextTermFilter(supplier, StandardEnglishPosTaggingTermFilter.getInstance());
        // Filter empty documents
        supplier = new DocumentFilteringSupplierDecorator(supplier, new DocumentFilter() {

            public boolean isDocumentGood(Document document) {
                SimpleTokenizedText text = document.getProperty(SimpleTokenizedText.class);
                DocumentName name = document.getProperty(DocumentName.class);
                DocumentURI uri = document.getProperty(DocumentURI.class);
                if ((text != null) && (text.getTokens().length > 0)) {
                    LOGGER.info("{} ({}) is accepted as part of the corpus", name != null ? name.get() : "null",
                            uri != null ? uri.get() : "null");
                    return true;
                } else {
                    LOGGER.info("{} ({}) is sorted out and won't be part of the corpus",
                            name != null ? name.get() : "null", uri != null ? uri.get() : "null");
                    return false;
                }
            }
        });
        return supplier;
    }

//    protected String generateCorpusName() {
//        String corpusName = this.corpusName;
//        switch (uriUsage) {
//        case CLASSES: {
//            corpusName += "_classes_";
//            break;
//        }
//        case EXTENDED_CLASSES: {
//            corpusName += "_eclasses_";
//            break;
//        }
//        case PROPERTIES: {
//            corpusName += "_prop_";
//            break;
//        }
//        case CLASSES_AND_PROPERTIES: {
//            corpusName += "_all_";
//            break;
//        }
//        case EXTENDED_CLASSES_AND_PROPERTIES: {
//            corpusName += "_eall_";
//            break;
//        }
//        }
//        corpusName += (wordOccurence == WordOccurence.UNIQUE) ? "unique.object" : "log.object";
//        return corpusName;
//    }

    public Corpus generateCorpusAndIndexWords(
            WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {
        return generateCorpusAndIndexWords(cachingLabelRetriever, null);
    }

    public Corpus generateCorpusAndIndexWords(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever,
            XmlWritingDocumentConsumer consumer) {
        DocumentSupplier supplier = generateCorpus(cachingLabelRetriever);

        Vocabulary vocabulary = new SimpleVocabulary();
        supplier = new SimpleWordIndexingSupplierDecorator(supplier, vocabulary);
        supplier = new DocumentWordCountingSupplierDecorator(supplier);

        if (consumer != null) {
            supplier = new DocumentConsumerAdaptingSupplierDecorator(supplier, consumer);
        }

        // Since this property is not serializeable we have to remove it
        List<Class<? extends DocumentProperty>> propertiesToRemove = new ArrayList<Class<? extends DocumentProperty>>();
        propertiesToRemove.add(DatasetVocabularies.class);
        propertiesToRemove.add(DatasetPropertyInfo.class);
        propertiesToRemove.add(DatasetSpecialClassesInfo.class);
        propertiesToRemove.add(DatasetClassInfo.class);
        propertiesToRemove.add(StringCountMapping.class);
        propertiesToRemove.add(SimpleTokenizedText.class);
        supplier = new PropertyRemovingSupplierDecorator(supplier, propertiesToRemove);

        ListCorpusCreator<List<Document>> preprocessor = new ListCorpusCreator<List<Document>>(supplier,
                new DocumentListCorpus<List<Document>>(new ArrayList<Document>()));
        Corpus corpus = preprocessor.getCorpus();
        corpus.addProperty(new CorpusVocabulary(vocabulary));
        return corpus;
    }

    public DocumentSupplier generateCorpus(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {
        DocumentSupplier supplier = readCorpus();
        supplier = useWhiteListFilter(supplier);
        supplier = generateDocuments(supplier, cachingLabelRetriever);
        supplier = filterStopWordsAndEmptyDocs(supplier);
        return supplier;
    }
}
