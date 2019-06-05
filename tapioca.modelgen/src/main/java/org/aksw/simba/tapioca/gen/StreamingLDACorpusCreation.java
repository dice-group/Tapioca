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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

public class StreamingLDACorpusCreation {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingLDACorpusCreation.class);

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
    // @Deprecated
    // private static final boolean EXPORT_CORPUS_AS_XML = false;

    public static void main(String[] args) {
        // create CLI Options object
        Options options = new Options();
        options.addOption("n", "input-file", true, "the input corpus file");
        options.addOption("o", "output-file", true, "the output corpus file");
        options.addOption("l", "label-file", true, "a label file that should be used to retrieve labels");
        options.addOption("c", "cache-file", true, "a cache file that can be used to cache labels retrieved via HTTP");
        options.addOption("h", "mongo-db-host", true,
                "the host name of a MongoDB instance containing URI to label mappings");
        options.addOption("p", "mongo-db-port", true,
                "the port of a MongoDB instance containing URI to label mappings");
        options.addOption("f", "fast", false,
                "the document processing is done in parallel");
        options.addOption("x", "export-xml", false, "export the corpus as XML");
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

        // UriUsage uriUsages[] = UriUsage.values();
        UriUsage uriUsages[] = new UriUsage[] { UriUsage.CLASSES_AND_PROPERTIES };
        WordOccurence wordOccurences[] = new WordOccurence[] { /* WordOccurence.UNIQUE, */WordOccurence.LOG };

        // String corpusName = CORPUS_NAME;

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
                    retrievers.add(FileBasedTokenizedLabelRetriever.create(file));
                }
            }
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
            cachingLabelRetriever = new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, cacheFiles,
                    retrievers);
            // LabelRetrievingDocumentSupplierDecorator cachingLabelRetriever;
            // cachingLabelRetriever = new
            // LabelRetrievingDocumentSupplierDecorator(null, false, labelsFiles);

            StreamingLDACorpusCreation corpusCreation;
            for (int i = 0; i < uriUsages.length; ++i) {
                for (int j = 0; j < wordOccurences.length; ++j) {
                    System.out.println(
                            "Starting corpus \"" + inputFile + "\" with " + uriUsages[i] + " and " + wordOccurences[j]);
                    corpusCreation = new StreamingLDACorpusCreation(inputFile, uriUsages[i], wordOccurences[j],
                            outputFile);
                    corpusCreation.run(cachingLabelRetriever, cmd.hasOption('f'));
                }
            }
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

    protected final String inputFile;
    protected final String outputFile;
    protected final UriUsage uriUsage;
    protected final WordOccurence wordOccurence;
    protected final boolean exportCorpusAsXml;

    public StreamingLDACorpusCreation(String inputFile, UriUsage uriUsage, WordOccurence wordOccurence,
            String outputFile) {
        this(inputFile, uriUsage, wordOccurence, outputFile, false);
    }

    public StreamingLDACorpusCreation(String inputFile, UriUsage uriUsage, WordOccurence wordOccurence,
            String outputFile, boolean exportCorpusAsXml) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.uriUsage = uriUsage;
        this.wordOccurence = wordOccurence;
        this.exportCorpusAsXml = exportCorpusAsXml;
    }

    public void run(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever, boolean isParallel) {

        XmlWritingDocumentConsumer consumer = null;
        if (exportCorpusAsXml) {
            consumer = XmlWritingDocumentConsumer.createXmlWritingDocumentConsumer(new File("./export.xml"));
        }

        Corpus corpus = generateCorpusAndIndexWords(cachingLabelRetriever, consumer, isParallel);

        cachingLabelRetriever.storeCache();
        if (consumer != null) {
            IOUtils.closeQuietly(consumer);
        }

        CorpusWriter writer = new GZipCorpusWriterDecorator(new CorpusObjectWriter());
        try {
            writer.writeCorpus(corpus, new File(outputFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Reads the corpus from the XML file created by the
     * {@link InitialCorpusCreation}.
     * 
     * @return DocumentSupplier managing the stream of documents.
     */
    protected Stream<Document> readCorpus() {
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
        return DocumentSupplier.convertToStream(supplier);
    }

    /**
     * Applies a white list filter if there is a white list filter file for this
     * corpus.
     * 
     * @param docStream
     * @return
     */
    protected Stream<Document> useWhiteListFilter(Stream<Document> docStream) {
        File whitelistFile = new File(inputFile.replace(".corpus", "_whitelist.txt"));
        if (whitelistFile.exists()) {
            try {
                final Set<String> whitelist = new HashSet<String>(FileUtils.readLines(whitelistFile));
                docStream = docStream.filter(new DocumentFilter() {
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
        return docStream;
    }

    /**
     * Generates tokenized documents based on their URI counts. URIs are filtered
     * and counted. After that their labels are retrieved and added to the documents
     * {@link SimpleTokenizedText} based on the {@link WordOccurence} used.
     * 
     * @param docStream
     * @param cachingLabelRetriever
     * @return
     */
    protected Stream<Document> generateDocuments(Stream<Document> docStream,
            WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {
        // Filter URIs
        docStream = filterUris(docStream);
        // Filter documents with missing property or class URIs
        // supplier = new DocumentFilteringSupplierDecorator(supplier, new
        // NoClassAndPropertyDocumentFilter());

        // Count the URIs
        docStream = docStream.map(new UriCountMappingCreatingDocumentSupplierDecorator(null, uriUsage));

        // Retrieve and tokenize the labels
        // LabelRetrievingDocumentSupplierDecorator cachingLabelRetriever = new
        // LabelRetrievingDocumentSupplierDecorator(
        // supplier);
        // Check whether there is a file containing labels
        docStream = docStream.map(cachingLabelRetriever);
        // supplier = new ExceptionCatchingDocumentSupplierDecorator(supplier);
        // Convert the counted tokens into tokenized text
        docStream = docStream
                .map(new StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator(null, wordOccurence));
        return docStream;
    }

    /**
     * Filters URIs based on the {@link VocabularyBlacklist}.
     * 
     * @param docStream
     * @return
     */
    protected Stream<Document> filterUris(Stream<Document> docStream) {
        Set<String> blacklist = VocabularyBlacklist.getInstance();
        return docStream
                .map(new UriFilteringDocumentSupplierDecorator<DatasetClassInfo>(null, blacklist,
                        DatasetClassInfo.class))
                .map(new SimpleBlankNodeRemovingDocumentSupplierDecorator<DatasetClassInfo>(null,
                        DatasetClassInfo.class))
                .map(new UriFilteringDocumentSupplierDecorator<DatasetPropertyInfo>(null, blacklist,
                        DatasetPropertyInfo.class))
                .map(new SimpleBlankNodeRemovingDocumentSupplierDecorator<DatasetPropertyInfo>(null,
                        DatasetPropertyInfo.class))
                .map(new SimpleBlankNodeRemovingDocumentSupplierDecorator<DatasetSpecialClassesInfo>(null,
                        DatasetSpecialClassesInfo.class));
    }

    protected Stream<Document> filterStopWordsAndEmptyDocs(Stream<Document> docStream) {
        // Filter the stop words
        docStream = docStream
                .map(new SimpleTokenizedTextTermFilter(null, StandardEnglishPosTaggingTermFilter.getInstance()));
        // Filter empty documents
        docStream = docStream.filter(new DocumentFilter() {

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
        return docStream;
    }

    public Corpus generateCorpusAndIndexWords(
            WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever, boolean isParallel) {
        return generateCorpusAndIndexWords(cachingLabelRetriever, null, isParallel);
    }

    public Corpus generateCorpusAndIndexWords(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever,
            XmlWritingDocumentConsumer consumer, boolean isParallel) {
        Stream<Document> docStream = generateCorpus(cachingLabelRetriever, isParallel);

        Vocabulary vocabulary = new SimpleVocabulary();
        docStream = docStream.map(new SimpleWordIndexingSupplierDecorator(null, vocabulary))
                .map(new DocumentWordCountingSupplierDecorator(null));

        if (consumer != null) {
            docStream = docStream.map(new DocumentConsumerAdaptingSupplierDecorator(null, consumer, true));
        }

        // Since this property is not serializeable we have to remove it
        List<Class<? extends DocumentProperty>> propertiesToRemove = new ArrayList<Class<? extends DocumentProperty>>();
        propertiesToRemove.add(DatasetVocabularies.class);
        propertiesToRemove.add(DatasetPropertyInfo.class);
        propertiesToRemove.add(DatasetSpecialClassesInfo.class);
        propertiesToRemove.add(DatasetClassInfo.class);
        propertiesToRemove.add(StringCountMapping.class);
        propertiesToRemove.add(SimpleTokenizedText.class);
        docStream = docStream.map(new PropertyRemovingSupplierDecorator(null, propertiesToRemove));

        Corpus corpus = new DocumentListCorpus<List<Document>>(docStream.collect(Collectors.toList()));
        corpus.addProperty(new CorpusVocabulary(vocabulary));
        return corpus;
    }

    public Stream<Document> generateCorpus(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever, boolean isParallel) {
        Stream<Document> docStream = readCorpus();
        if(isParallel) {
            docStream = docStream.parallel();
        }
        useWhiteListFilter(docStream);
        generateDocuments(docStream, cachingLabelRetriever);
        filterStopWordsAndEmptyDocs(docStream);
        return docStream;
    }
}
