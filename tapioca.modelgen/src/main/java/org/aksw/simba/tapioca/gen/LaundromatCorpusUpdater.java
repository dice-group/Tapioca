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
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.data.DatasetSpecialClassesInfo;
import org.aksw.simba.tapioca.data.DatasetVocabularies;
import org.aksw.simba.topicmodeling.io.xml.XmlWritingDocumentConsumer;
import org.aksw.simba.topicmodeling.io.xml.stream.StreamBasedXmlDocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Updates the URIs of the datasets / documents based on a TSV file.
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class LaundromatCorpusUpdater {

    private static final Logger LOGGER = LoggerFactory.getLogger(LaundromatCorpusUpdater.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Not enough arguments. Call the program as:");
            System.err
                    .println("LaundromatCorpusUpdater <laundromat-tsv-file> <input-corpus-file> <output-corpus-file>");
            System.exit(1);
        }
        LaundromatCorpusUpdater updater = new LaundromatCorpusUpdater();
        updater.run(new File(args[0]), new File(args[1]), new File(args[2]));
    }

    protected void run(File laundromatTSVFile, File inputFolder, File outputFile) {
        XmlWritingDocumentConsumer.registerParseableDocumentProperty(DatasetClassInfo.class);
        XmlWritingDocumentConsumer.registerParseableDocumentProperty(DatasetSpecialClassesInfo.class);
        XmlWritingDocumentConsumer.registerParseableDocumentProperty(DatasetPropertyInfo.class);
        XmlWritingDocumentConsumer.registerParseableDocumentProperty(DatasetVocabularies.class);

        DocumentSupplier supplier = StreamBasedXmlDocumentSupplier.createReader(inputFolder);
        supplier = LaundromatDocumentUpdater.create(supplier, laundromatTSVFile);
        if (supplier == null) {
            LOGGER.error("The LaundromatDocumentUpdater couldn't be created. Aborting.");
            return;
        }

        XmlWritingDocumentConsumer consumer = XmlWritingDocumentConsumer
                .createXmlWritingDocumentConsumer((outputFile).getAbsoluteFile());

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

    public static class LaundromatDocumentUpdater extends AbstractDocumentSupplierDecorator {

        public static final String LAUNDROMAT_URI_PREFIX = "http://lodlaundromat.org/resource/";
        public static final int LAUNDROMAT_URI_PREFIX_LENGTH = LAUNDROMAT_URI_PREFIX.length();
        /*
         * It is unclear why the laundromat TSV file is not a real TSV file but contains
         * only whitespaces instead of tab characters. However, the following IDs should
         * work for the complete file
         */
        public static final int URI_ID = 14;
        public static final int HASH_ID = 6;

        public static LaundromatDocumentUpdater create(DocumentSupplier documentSource, File tsvFile) {
            Map<String, String> hash2Uri = new HashMap<>();
            try (FileReader fReader = new FileReader(tsvFile)) {
                CSVReader reader = new CSVReader(fReader, ' ');
                String line[] = reader.readNext();
                while (line != null) {
                    if ((line.length > URI_ID) && (line.length > HASH_ID)) {
                        hash2Uri.put(line[HASH_ID], line[URI_ID]);
                    } else {
                        LOGGER.info("Discarded " + line.toString());
                    }
                    line = reader.readNext();
                }
                reader.close();
                return new LaundromatDocumentUpdater(documentSource, hash2Uri);
            } catch (Exception e) {
                LOGGER.error("Exception while creating LaundromatDocumentUpdater. Returning null.", e);
                return null;
            }
        }

        protected Map<String, String> hash2Uri;

        public LaundromatDocumentUpdater(DocumentSupplier documentSource, Map<String, String> hash2Uri) {
            super(documentSource);
            this.hash2Uri = hash2Uri;
            LOGGER.info("Initialized with " + hash2Uri.size() + " elements.");
        }

        @Override
        protected Document prepareDocument(Document document) {
            DocumentURI uri = document.getProperty(DocumentURI.class);
            if (uri == null) {
                LOGGER.error("Got a document without the necessary DocumentURI property.");
                return document;
            }
            String hash = extractHash(uri);
            if (!hash2Uri.containsKey(hash)) {
                LOGGER.error("Got a document with an unknown hash (\"" + hash + "\").");
                return document;
            }
            uri.set(hash2Uri.get(hash));
            return document;
        }

        protected String extractHash(DocumentURI uri) {
            return uri.get().trim().substring(LAUNDROMAT_URI_PREFIX_LENGTH);
        }
    }
}
