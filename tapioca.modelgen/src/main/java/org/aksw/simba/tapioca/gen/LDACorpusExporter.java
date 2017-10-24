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
import java.util.ArrayList;
import java.util.List;

import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.data.DatasetSpecialClassesInfo;
import org.aksw.simba.tapioca.data.DatasetVocabularies;
import org.aksw.simba.tapioca.data.SimpleTokenizedText;
import org.aksw.simba.tapioca.data.StringCountMapping;
import org.aksw.simba.tapioca.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.WordOccurence;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.io.xml.XmlWritingDocumentConsumer;
import org.aksw.simba.topicmodeling.preprocessing.ListCorpusCreator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentConsumerAdaptingSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.PropertyRemovingSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.corpus.DocumentListCorpus;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentProperty;
import org.apache.commons.io.IOUtils;

/**
 * Generates a human readable corpus. This class should work as the
 * {@link LDACorpusCreation}
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class LDACorpusExporter extends LDACorpusCreation {

	public static void main(String[] args) {
		// UriUsage uriUsages[] = UriUsage.values();
		UriUsage uriUsages[] = new UriUsage[] { UriUsage.CLASSES_AND_PROPERTIES };
		WordOccurence wordOccurences[] = new WordOccurence[] {
		// WordOccurence.UNIQUE,
		WordOccurence.LOG };

		File labelsFiles[] = new File[] { new File(CORPUS_FILE.replace(".corpus", ".labels.object")),
				new File(CORPUS_FILE.replace(".corpus", ".ret_labels_1.object")) };
		WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever;
		cachingLabelRetriever = new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, CACHE_FILES, labelsFiles);

		LDACorpusExporter corpusExporter;
		for (int i = 0; i < uriUsages.length; ++i) {
			for (int j = 0; j < wordOccurences.length; ++j) {
				System.out.println("Starting corpus \"" + CORPUS_NAME + "\" with " + uriUsages[i] + " and "
						+ wordOccurences[j]);
				corpusExporter = new LDACorpusExporter(CORPUS_NAME, CORPUS_FILE, uriUsages[i], wordOccurences[j]);
				corpusExporter.run(cachingLabelRetriever);
			}
		}
		cachingLabelRetriever.close();
	}

	public LDACorpusExporter(String corpusName, String corpusFile, UriUsage uriUsage, WordOccurence wordOccurence) {
		super(corpusName, corpusFile, uriUsage, wordOccurence);
	}

	public void run(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {
		String corpusName = generateCorpusName();

		DocumentSupplier supplier = readCorpus();
		supplier = useWhiteListFilter(supplier);
		supplier = generateDocuments(supplier, cachingLabelRetriever);
		supplier = filterStopWordsAndEmptyDocs(supplier);

		// Since this property is not serializeable we have to remove it
		List<Class<? extends DocumentProperty>> propertiesToRemove = new ArrayList<Class<? extends DocumentProperty>>();
		propertiesToRemove.add(DatasetVocabularies.class);
		propertiesToRemove.add(DatasetPropertyInfo.class);
		propertiesToRemove.add(DatasetSpecialClassesInfo.class);
		propertiesToRemove.add(DatasetClassInfo.class);
		propertiesToRemove.add(StringCountMapping.class);
		supplier = new PropertyRemovingSupplierDecorator(supplier, propertiesToRemove);

		XmlWritingDocumentConsumer consumer = null;
		XmlWritingDocumentConsumer.registerParseableDocumentProperty(SimpleTokenizedText.class);
		consumer = XmlWritingDocumentConsumer.createXmlWritingDocumentConsumer(new File((new File(this.corpusFile))
				.getParentFile().getAbsolutePath() + File.separator + corpusName + "_export.xml"));
		supplier = new DocumentConsumerAdaptingSupplierDecorator(supplier, consumer);

		ListCorpusCreator<List<Document>> preprocessor = new ListCorpusCreator<List<Document>>(supplier,
				new DocumentListCorpus<List<Document>>(new ArrayList<Document>()));
		preprocessor.getCorpus();

		cachingLabelRetriever.storeCache();
		if (consumer != null) {
			IOUtils.closeQuietly(consumer);
		}
	}
}
