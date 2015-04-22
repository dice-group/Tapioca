package org.aksw.simba.tapioca.gen;

import java.io.File;
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
import org.aksw.simba.tapioca.gen.data.DatasetURIs;
import org.aksw.simba.tapioca.gen.preprocessing.DatasetURIsSummarizingSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.topicmodeling.io.CorpusObjectWriter;
import org.aksw.simba.topicmodeling.io.gzip.GZipCorpusObjectReader;
import org.aksw.simba.topicmodeling.io.gzip.GZipCorpusObjectWriter;
import org.aksw.simba.topicmodeling.io.xml.stream.StreamBasedXmlDocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.ListCorpusCreator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentFilteringSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.PropertyRemovingSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.filter.DocumentFilter;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.aksw.simba.topicmodeling.utils.corpus.DocumentListCorpus;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentName;
import org.aksw.simba.topicmodeling.utils.doc.DocumentProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URIBasedIndexGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(URIBasedIndexGenerator.class);

    public static final String BL_CORPUS_FILE = TMBasedIndexGenerator.TAPIOCA_FOLDER
            + TMBasedIndexGenerator.CORPUS_NAME + "_BL.object";

    public static final String FINAL_CORPUS_FILE = TMBasedIndexGenerator.CORPUS_NAME + "_BL_final.corpus";

    public static void main(String[] args) {
        URIBasedIndexGenerator generator = new URIBasedIndexGenerator();
        generator.run();
    }

    public void run() {
        File outputFolder = new File(TMBasedIndexGenerator.OUTPUT_FOLDER);
        if (!outputFolder.exists()) {
            outputFolder.mkdirs();
        }

        File datasetDescriptionsFile = new File(TMBasedIndexGenerator.OUTPUT_FOLDER + File.separator
                + FINAL_CORPUS_FILE);
        if (datasetDescriptionsFile.exists()) {
            LOGGER.info("The final corpus file is already existing.");
        } else {
            generateFinalCorpusFile();
        }
    }

    protected void generateFinalCorpusFile() {
        if (checkBLCorpusExistence()) {
            MetaDataInformationCollector collector = new MetaDataInformationCollector();
            LOGGER.info("Generating final corpus file...");
            collector.run(TMBasedIndexGenerator.META_DATA_FILE, BL_CORPUS_FILE, TMBasedIndexGenerator.STAT_RESULT_FILE,
                    TMBasedIndexGenerator.OUTPUT_FOLDER + File.separator + FINAL_CORPUS_FILE,
                    TMBasedIndexGenerator.OUTPUT_FOLDER + File.separator + TMBasedIndexGenerator.MODEL_META_DATA_FILE);
        }
    }

    protected boolean checkBLCorpusExistence() {
        File blCorpusFile = new File(BL_CORPUS_FILE);
        if (!blCorpusFile.exists()) {
            LOGGER.warn("The BL corpus file is not existing. Trying to generate it...");
            generateBLCorpusFile();
            if (!blCorpusFile.exists()) {
                LOGGER.error("The BL corpus file is not existing and couldn't be generated.");
                return false;
            }
        }
        return true;
    }

    protected void generateBLCorpusFile() {
        DocumentSupplier supplier = StreamBasedXmlDocumentSupplier.createReader(new File(
                TMBasedIndexGenerator.CORPUS_FILE), true);
        StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetClassInfo.class);
        StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetSpecialClassesInfo.class);
        StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetPropertyInfo.class);
        StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetVocabularies.class);
        // Count the URIs
        supplier = new UriCountMappingCreatingDocumentSupplierDecorator(supplier, UriUsage.CLASSES_AND_PROPERTIES);

        supplier = new DatasetURIsSummarizingSupplierDecorator(supplier);

        supplier = new DocumentFilteringSupplierDecorator(supplier, new DocumentFilter() {
            public boolean isDocumentGood(Document document) {
                DatasetURIs uris = document.getProperty(DatasetURIs.class);
                return (uris != null) && (uris.get().size() > 0);
            }
        });

        // final Set<String> whiteList = generateDocumentNameWhiteList();
        // if (whiteList != null) {
        // supplier = new DocumentFilteringSupplierDecorator(supplier, new
        // DocumentFilter() {
        // public boolean isDocumentGood(Document document) {
        // DocumentName name = document.getProperty(DocumentName.class);
        // return (name != null) && (whiteList.contains(name.get()));
        // }
        // });
        // }

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

        CorpusObjectWriter writer = new GZipCorpusObjectWriter(new File(BL_CORPUS_FILE));
        writer.writeCorpus(preprocessor.getCorpus());
    }

//    protected Set<String> generateDocumentNameWhiteList() {
//        File finalLDACorpusFile = new File(TMBasedIndexGenerator.LDA_CORPUS_FILE);
//        if (!finalLDACorpusFile.exists()) {
//            LOGGER.info("The LDA corpus file is not existing. Can't use it as white list.");
//            return null;
//        }
//        // GZipCorpusObjectReader reader = new
//        // GZipCorpusObjectReader(finalLDACorpusFile);
//        // Corpus finalLDACorpus = reader.getCorpus();
//        Set<String> whiteList = new HashSet<String>();
//        DocumentName name;
//        DocumentSupplier supplier = StreamBasedXmlDocumentSupplier.createReader(new File(
//                TMBasedIndexGenerator.CORPUS_FILE), true);
//        Document document = supplier.getNextDocument();
//        while (document != null) {
//            name = document.getProperty(DocumentName.class);
//            if (name != null) {
//                whiteList.add(name.get());
//            }
//            document = supplier.getNextDocument();
//        }
//        return whiteList;
//    }
}
