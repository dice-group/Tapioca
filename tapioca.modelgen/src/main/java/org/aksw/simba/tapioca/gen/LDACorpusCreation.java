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
import org.aksw.simba.topicmodeling.io.gzip.GZipCorpusObjectWriter;
import org.aksw.simba.topicmodeling.io.xml.XmlWritingDocumentConsumer;
import org.aksw.simba.topicmodeling.io.xml.stream.StreamBasedXmlDocumentSupplier;
import org.aksw.simba.topicmodeling.lang.postagging.StandardEnglishPosTaggingTermFilter;
import org.aksw.simba.topicmodeling.preprocessing.ListCorpusCreator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentConsumerAdaptingSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentFilteringSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentWordCountingSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.PropertyRemovingSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.filter.DocumentFilter;
import org.aksw.simba.topicmodeling.utils.corpus.Corpus;
import org.aksw.simba.topicmodeling.utils.corpus.DocumentListCorpus;
import org.aksw.simba.topicmodeling.utils.corpus.properties.CorpusVocabulary;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentName;
import org.aksw.simba.topicmodeling.utils.doc.DocumentProperty;
import org.aksw.simba.topicmodeling.utils.doc.DocumentURI;
import org.aksw.simba.topicmodeling.utils.vocabulary.SimpleVocabulary;
import org.aksw.simba.topicmodeling.utils.vocabulary.Vocabulary;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LDACorpusCreation {

	private static final Logger LOGGER = LoggerFactory.getLogger(LDACorpusCreation.class);

	public static final File CACHE_FILES[] = new File[] { new File("C:/Daten/tapioca/cache/uriToLabelCache_1.object"),
			new File("C:/Daten/tapioca/cache/uriToLabelCache_2.object"),
			new File("C:/Daten/tapioca/cache/uriToLabelCache_3.object") };
	// public static final File CACHE_FILES[] = new File[] { new
	// File("/home/mroeder/tapioca/uriToLabelCache_1.object"),
	// new File("/home/mroeder/tapioca/uriToLabelCache_2.object"),
	// new File("/home/mroeder/tapioca/uriToLabelCache_3.object") };

	// public static final String CORPUS_NAME = "lodStatsGold";
	public static final String CORPUS_NAME = "lodDiagram";
	public static final String CORPUS_FILE = "/Daten/tapioca/" + CORPUS_NAME + ".corpus";

	private static final boolean EXPORT_CORPUS_AS_XML = false;

	public static void main(String[] args) {
		// HttpClient client = HttpOp.getDefaultHttpClient();
		// HttpClientBuilder hcbuilder = HttpClientBuilder.create();
		// hcbuilder.useSystemProperties();
		// hcbuilder.setRetryHandler(new StandardHttpRequestRetryHandler(1,
		// true));
		// HttpOp.setDefaultHttpClient(hcbuilder.build());
		// System.setProperty(org.apache.http.params.CoreConnectionPNames.CONNECTION_TIMEOUT,
		// "60000");
		UriUsage uriUsages[] = UriUsage.values();
		// UriUsage uriUsages[] = new UriUsage[] {
		// UriUsage.CLASSES_AND_PROPERTIES };
		WordOccurence wordOccurences[] = new WordOccurence[] { WordOccurence.UNIQUE, WordOccurence.LOG };

		String corpusName = CORPUS_NAME;

		File labelsFiles[] = new File[] { new File(CORPUS_FILE.replace(".corpus", ".labels.object")),
				new File(CORPUS_FILE.replace(".corpus", ".ret_labels_1.object")) };
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
				corpusCreation = new LDACorpusCreation(corpusName, CORPUS_FILE, uriUsages[i], wordOccurences[j]);
				corpusCreation.run(cachingLabelRetriever);
			}
		}
		cachingLabelRetriever.close();
	}

	protected final String corpusName;
	protected final String corpusFile;
	protected final UriUsage uriUsage;
	protected final WordOccurence wordOccurence;
	protected final boolean exportCorpusAsXml;

	public LDACorpusCreation(String corpusName, String corpusFile, UriUsage uriUsage, WordOccurence wordOccurence) {
		this.corpusName = corpusName;
		this.corpusFile = corpusFile;
		this.uriUsage = uriUsage;
		this.wordOccurence = wordOccurence;
		this.exportCorpusAsXml = EXPORT_CORPUS_AS_XML;
	}

	public LDACorpusCreation(String corpusName, String corpusFile, UriUsage uriUsage, WordOccurence wordOccurence,
			boolean exportCorpusAsXml) {
		this.corpusName = corpusName;
		this.corpusFile = corpusFile;
		this.uriUsage = uriUsage;
		this.wordOccurence = wordOccurence;
		this.exportCorpusAsXml = exportCorpusAsXml;
	}

	public void run(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {
		String corpusName = generateCorpusName();

		XmlWritingDocumentConsumer consumer = null;
		if (exportCorpusAsXml) {
			consumer = XmlWritingDocumentConsumer.createXmlWritingDocumentConsumer(new File("./export.xml"));
		}

		Corpus corpus = generateCorpusAndIndexWords(cachingLabelRetriever, consumer);

		cachingLabelRetriever.storeCache();
		if (consumer != null) {
			IOUtils.closeQuietly(consumer);
		}

		CorpusObjectWriter writer = new GZipCorpusObjectWriter(new File(corpusName));
		writer.writeCorpus(corpus);
	}

	/**
	 * Reads the corpus from the XML file created by the
	 * {@link InitialCorpusCreation}.
	 * 
	 * @return DocumentSupplier managing the stream of documents.
	 */
	protected DocumentSupplier readCorpus() {
		DocumentSupplier supplier = StreamBasedXmlDocumentSupplier.createReader(new File(corpusFile), true);
		StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetClassInfo.class);
		StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetSpecialClassesInfo.class);
		StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetPropertyInfo.class);
		StreamBasedXmlDocumentSupplier.registerParseableDocumentProperty(DatasetVocabularies.class);

		supplier = new DocumentFilteringSupplierDecorator(supplier, new DocumentFilter() {
			public boolean isDocumentGood(Document document) {
				DocumentName name = document.getProperty(DocumentName.class);
				DocumentURI uri = document.getProperty(DocumentURI.class);
				LOGGER.info("Processing of {} ({}) starts", name != null ? name.get() : "null", uri != null ? uri.get()
						: "null");
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
		File whitelistFile = new File(corpusFile.replace(".corpus", "_whitelist.txt"));
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
		return supplier;
	}

	/**
	 * Generates tokenized documents based on their URI counts. URIs are
	 * filtered and counted. After that their labels are retrieved and added to
	 * the documents {@link SimpleTokenizedText} based on the
	 * {@link WordOccurence} used.
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
					LOGGER.info("{} ({}) is sorted out and won't be part of the corpus", name != null ? name.get() : "null",
							uri != null ? uri.get() : "null");
					return false;
				}
			}
		});
		return supplier;
	}

	protected String generateCorpusName() {
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
		return corpusName;
	}

	public Corpus generateCorpusAndIndexWords(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {
		return generateCorpusAndIndexWords(cachingLabelRetriever, null);
	}

	public Corpus generateCorpusAndIndexWords(
			WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever,
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
