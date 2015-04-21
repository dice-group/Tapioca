package org.aksw.simba.tapioca.gen;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.data.DatasetSpecialClassesInfo;
import org.aksw.simba.tapioca.data.DatasetVocabularies;
import org.aksw.simba.tapioca.preprocessing.JenaBasedVoidParsingSupplierDecorator;
import org.aksw.simba.topicmodeling.io.FolderReader;
import org.aksw.simba.topicmodeling.io.xml.XmlWritingDocumentConsumer;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentTextCreatingSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.PropertyRemovingSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentRawData;
import org.aksw.simba.topicmodeling.utils.doc.DocumentText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitialCorpusCreation {

    private static final Logger LOGGER = LoggerFactory.getLogger(InitialCorpusCreation.class);

    // public static final String CORPUS_NAME = "lod";
    // public static final String CORPUS_NAME = "synthUniVsDBpUnis";
    // public static final String CORPUS_NAME = "CrawledRdfData";
    public static final String CORPUS_NAME = "lodStats";
    // public static final String CORPUS_NAME = "DataHub";
    public static final String CORPUS_FILE = "C:/Daten/tapioca/" + CORPUS_NAME + ".corpus";

    // public static final File INPUT_FOLDER = new File("pages");
    // public static final File INPUT_FOLDER = new File("void_Corpora/" +
    // CORPUS_NAME);
    // public static final File INPUT_FOLDER = new
    // File("/data/m.roeder/daten/CrawledRdfData/voidFiles");
    // public static final File INPUT_FOLDER = new File(
    // "F:/data/daten/CrawledRdfData/voidFiles");
    // public static final File INPUT_FOLDER = new
    // File("C:/Daten/Dropbox/lodstats-rdf/23032015/void");

    // public static final boolean PARSE_FROM_LOD_STATS = false;
    public static final File INPUT_FOLDER = new File("C:/Daten/Dropbox/lodstats-rdf/23032015/void");

    public static void main(String[] args) {
        InitialCorpusCreation creation = new InitialCorpusCreation();
        creation.run(CORPUS_FILE, INPUT_FOLDER);
    }

    protected void run(String corpusFile, File inputFolder) {
        FolderReader reader = new FolderReader(inputFolder);
        reader.setUseFolderNameAsCategory(true);
        DocumentSupplier supplier = reader;
        supplier = new DocumentTextCreatingSupplierDecorator(supplier);
        // if (PARSE_FROM_LOD_STATS) {
        // supplier = new LodStatVoidParsingSupplierDecorator(supplier);
        // }
        supplier = new JenaBasedVoidParsingSupplierDecorator(supplier);
        supplier = new PropertyRemovingSupplierDecorator(supplier, Arrays.asList(DocumentRawData.class,
                DocumentText.class));

        XmlWritingDocumentConsumer consumer = XmlWritingDocumentConsumer.createXmlWritingDocumentConsumer((new File(
                corpusFile)).getAbsoluteFile());
        XmlWritingDocumentConsumer.registerParseableDocumentProperty(DatasetClassInfo.class);
        XmlWritingDocumentConsumer.registerParseableDocumentProperty(DatasetSpecialClassesInfo.class);
        XmlWritingDocumentConsumer.registerParseableDocumentProperty(DatasetPropertyInfo.class);
        XmlWritingDocumentConsumer.registerParseableDocumentProperty(DatasetVocabularies.class);

        Document document = supplier.getNextDocument();
        int count = 0;
        while (document != null) {
            try {
                consumer.consumeDocument(document);
            } catch (Exception e) {
                LOGGER.error("Exception at document #" + document.getDocumentId() + ". Aborting.", e);
                return;
            }
            ++count;
            if ((count % 100) == 0) {
                LOGGER.info("Saw " + count + " documents");
            }
            document = supplier.getNextDocument();
        }
        LOGGER.info("Saw " + count + " documents");
        try {
            consumer.close();
        } catch (IOException e) {
            LOGGER.warn("Got an exception while closing the XML Writer.", e);
        }
    }
}
