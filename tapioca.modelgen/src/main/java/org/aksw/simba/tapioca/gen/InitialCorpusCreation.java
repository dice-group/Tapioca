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
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentFilteringSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentTextCreatingSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.PropertyRemovingSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.filter.StringContainingDocumentPropertyBasedFilter;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.filter.StringContainingDocumentPropertyBasedFilter.StringContainingDocumentPropertyBasedFilterType;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentName;
import org.aksw.simba.topicmodeling.utils.doc.DocumentRawData;
import org.aksw.simba.topicmodeling.utils.doc.DocumentText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a new corpus file based on the void information from a given
 * directory.
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class InitialCorpusCreation {

    private static final Logger LOGGER = LoggerFactory.getLogger(InitialCorpusCreation.class);

    // public static final String CORPUS_NAME = "lod";
    // public static final String CORPUS_NAME = "synthUniVsDBpUnis";
    // public static final String CORPUS_NAME = "CrawledRdfData";
    @Deprecated
    public static final String CORPUS_NAME = "lodStats";
    // public static final String CORPUS_NAME = "DataHub";
    @Deprecated
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
    @Deprecated
    public static final File INPUT_FOLDER = new File("C:/Daten/Dropbox/lodstats-rdf/23032015/void");

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Not enough arguments. Call the program as:");
            System.err.println("InitialCorpusCreation <input-directory> <output-corpus-file>");
            System.exit(1);
        }
        InitialCorpusCreation creation = new InitialCorpusCreation();
        creation.run(new File(args[0]), new File(args[1]));
    }

    protected void run(File inputFolder, File corpusFile) {
        FolderReader reader = new FolderReader(inputFolder);
        reader.setUseFolderNameAsCategory(true);
        DocumentSupplier supplier = reader;
        // Remove all files which do not end with .ttl
        supplier = new DocumentFilteringSupplierDecorator(supplier, new StringContainingDocumentPropertyBasedFilter<>(
                StringContainingDocumentPropertyBasedFilterType.ENDS_WITH, DocumentName.class, ".ttl", true));
        
        supplier = new DocumentTextCreatingSupplierDecorator(supplier);
        // if (PARSE_FROM_LOD_STATS) {
        // supplier = new LodStatVoidParsingSupplierDecorator(supplier);
        // }
        supplier = new JenaBasedVoidParsingSupplierDecorator(supplier);
        supplier = new PropertyRemovingSupplierDecorator(supplier,
                Arrays.asList(DocumentRawData.class, DocumentText.class));

        XmlWritingDocumentConsumer consumer = XmlWritingDocumentConsumer
                .createXmlWritingDocumentConsumer(corpusFile.getAbsoluteFile());
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
