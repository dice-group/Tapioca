package org.aksw.simba.tapioca.gen;

import java.io.File;
import java.io.IOException;
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
import org.aksw.simba.tapioca.preprocessing.labelretrieving.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.io.CorpusObjectWriter;
import org.aksw.simba.topicmodeling.io.xml.XmlWritingDocumentConsumer;
import org.aksw.simba.topicmodeling.io.xml.stream.StreamBasedXmlDocumentSupplier;
import org.aksw.simba.topicmodeling.lang.postagging.StandardEnglishPosTaggingTermFilter;
import org.aksw.simba.topicmodeling.preprocessing.ListCorpusCreator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentConsumerAdaptingSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentFilteringSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.PropertyRemovingSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.filter.DocumentFilter;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.aksw.simba.topicmodeling.utils.corpus.DocumentListCorpus;
import org.aksw.simba.topicmodeling.utils.corpus.properties.CorpusVocabulary;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentName;
import org.aksw.simba.topicmodeling.utils.doc.DocumentProperty;
import org.aksw.simba.topicmodeling.utils.vocabulary.SimpleVocabulary;
import org.aksw.simba.topicmodeling.utils.vocabulary.Vocabulary;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LDACorpusCreation {

    private static final Logger LOGGER = LoggerFactory.getLogger(LDACorpusCreation.class);

//    public static final File CACHE_FILES[] = new File[] { new File("C:/Daten/tapioca/cache/uriToLabelCache_1.object"),
//            new File("C:/Daten/tapioca/cache/uriToLabelCache_2.object"),
//            new File("C:/Daten/tapioca/cache/uriToLabelCache_3.object") };
    public static final File CACHE_FILES[] = new File[] { new File("/home/mroeder/tapioca/uriToLabelCache_1.object"),
        new File("/home/mroeder/tapioca/uriToLabelCache_2.object"),
        new File("/home/mroeder/tapioca/uriToLabelCache_3.object") };

    public static void main(String[] args) {
        // HttpClient client = HttpOp.getDefaultHttpClient();
        // HttpClientBuilder hcbuilder = HttpClientBuilder.create();
        // hcbuilder.useSystemProperties();
        // hcbuilder.setRetryHandler(new StandardHttpRequestRetryHandler(1,
        // true));
        // HttpOp.setDefaultHttpClient(hcbuilder.build());
        // System.setProperty(org.apache.http.params.CoreConnectionPNames.CONNECTION_TIMEOUT,
        // "60000");
        // UriUsage uriUsages[] = UriUsage.values();
        UriUsage uriUsages[] = new UriUsage[] { UriUsage.CLASSES_AND_PROPERTIES };
        WordOccurence wordOccurences[] = new WordOccurence[] { WordOccurence.LOG };

        String corpusName = InitialCorpusCreation.CORPUS_NAME;

        File labelsFiles[] = new File[] {
                new File(InitialCorpusCreation.CORPUS_FILE.replace(".corpus", ".labels.object")),
                new File(InitialCorpusCreation.CORPUS_FILE.replace(".corpus", ".ret_labels_1.object")) };
        WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever;
        cachingLabelRetriever = new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, CACHE_FILES, labelsFiles);
        // LabelRetrievingDocumentSupplierDecorator cachingLabelRetriever;
        // cachingLabelRetriever = new
        // LabelRetrievingDocumentSupplierDecorator(null, false, labelsFiles);

        LDACorpusCreation corpusCreation;
        for (int i = 0; i < uriUsages.length; ++i) {
            for (int j = 0; j < wordOccurences.length; ++j) {
                System.out.println("Starting corpus \"" + corpusName + "\" with " + uriUsages[i] + " and "
                        + wordOccurences[j]);
                corpusCreation = new LDACorpusCreation(corpusName, uriUsages[i], wordOccurences[j]);
                corpusCreation.run(cachingLabelRetriever);
            }
        }
        cachingLabelRetriever.close();
    }

    private final String corpusName;
    private final UriUsage uriUsage;
    private final WordOccurence wordOccurence;

    public LDACorpusCreation(String corpusName, UriUsage uriUsage, WordOccurence wordOccurence) {
        this.corpusName = corpusName;
        this.uriUsage = uriUsage;
        this.wordOccurence = wordOccurence;
    }

    public void run(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {
        String corpusName = this.corpusName;
        switch (uriUsage) {
        case CLASSES: {
            corpusName += "_classes_";
            break;
        }
        case EXTENDED_CLASSES: {
            corpusName += "_eclasses_";
            break;
        }
        case PROPERTIES: {
            corpusName += "_prop_";
            break;
        }
        case CLASSES_AND_PROPERTIES: {
            corpusName += "_all_";
            break;
        }
        case EXTENDED_CLASSES_AND_PROPERTIES: {
            corpusName += "_eall_";
            break;
        }
        }
        corpusName += (wordOccurence == WordOccurence.UNIQUE) ? "unique.object" : "log.object";

        DocumentSupplier supplier = StreamBasedXmlDocumentSupplier.createReader(new File(
                InitialCorpusCreation.CORPUS_FILE), true);
        StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetClassInfo.class);
        StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetSpecialClassesInfo.class);
        StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetPropertyInfo.class);
        StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetVocabularies.class);

        supplier = new DocumentFilteringSupplierDecorator(supplier, new DocumentFilter() {
            public boolean isDocumentGood(Document document) {
                LOGGER.info("Processing of {} starts", document.getProperty(DocumentName.class).get());
                return true;
            }
        });

        File whitelistFile = new File(InitialCorpusCreation.CORPUS_FILE.replace(".corpus", "_whitelist.txt"));
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
                            return whitelist.contains(name);
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

        // Filter URIs
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
        // Filter the stop words
        supplier = new SimpleTokenizedTextTermFilter(supplier, StandardEnglishPosTaggingTermFilter.getInstance());
        // Filter empty documents
        supplier = new DocumentFilteringSupplierDecorator(supplier, new DocumentFilter() {
            // private Set<String> temp = new
            // HashSet<String>(Arrays.asList("http://isocat",
            // "http://globalnames",
            // "http://bio2rdf-sgd", "http://southampton-ac-uk-apps",
            // "http://bio2rdf-biomodels",
            // "http://pub-bielefeld", "http://zhishi-me",
            // "http://bio2rdf-hgnc", "http://open-library",
            // "http://nalt", "http://bio2rdf-pubchem", "http://bio2rdf-omim"));

            public boolean isDocumentGood(Document document) {
                SimpleTokenizedText text = document.getProperty(SimpleTokenizedText.class);
                // System.out.println(document.getProperty(DocumentName.class)
                // + (temp.contains(document.getProperty(DocumentName.class)) ?
                // " - " : " + ")
                // + text.getTokens().length);
                if ((text != null) && (text.getTokens().length > 0)) {
                    LOGGER.info("{} is accepted as part of the corpus", document.getProperty(DocumentName.class).get());
                    return true;
                } else {
                    LOGGER.info("{} is sorted out and won't be part of the corpus",
                            document.getProperty(DocumentName.class).get());
                    return false;
                }
                // return (text != null) && (text.getTokens().length > 0);
            }
        });

        Vocabulary vocabulary = new SimpleVocabulary();
        supplier = new SimpleWordIndexingSupplierDecorator(supplier, vocabulary);

        supplier = new DocumentConsumerAdaptingSupplierDecorator(supplier,
                XmlWritingDocumentConsumer.createXmlWritingDocumentConsumer(new File("./export.xml")));

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

        cachingLabelRetriever.storeCache();

        corpus.addProperty(new CorpusVocabulary(vocabulary));

        CorpusObjectWriter writer = new CorpusObjectWriter(new File(corpusName));
        writer.writeCorpus(corpus);
    }
}
