/**
 * This file is part of tapioca.indexgenerator.
 *
 * tapioca.indexgenerator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.indexgenerator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.indexgenerator.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.indexgenerator.docgen;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.aksw.simba.tapioca.cores.data.DatasetClassInfo;
import org.aksw.simba.tapioca.cores.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.cores.data.DatasetSpecialClassesInfo;
import org.aksw.simba.tapioca.cores.data.DatasetVocabularies;
import org.aksw.simba.tapioca.cores.data.SimpleTokenizedText;
import org.aksw.simba.tapioca.cores.data.StringCountMapping;
import org.aksw.simba.tapioca.cores.data.VocabularyBlacklist;
import org.aksw.simba.tapioca.cores.preprocessing.SimpleBlankNodeRemovingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.SimpleTokenizedTextTermFilter;
import org.aksw.simba.tapioca.cores.preprocessing.SimpleWordIndexingSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.WordOccurence;
import org.aksw.simba.tapioca.cores.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.tapioca.cores.preprocessing.UriFilteringDocumentSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.io.CorpusObjectWriter;
import org.aksw.simba.topicmodeling.io.gzip.GZipCorpusObjectWriter;
import org.aksw.simba.topicmodeling.io.xml.XmlWritingDocumentConsumer;
import org.aksw.simba.topicmodeling.io.xml.stream.StreamBasedXmlDocumentSupplier;
import org.aksw.simba.topicmodeling.lang.postagging.StandardEnglishPosTaggingTermFilter;
import org.aksw.simba.topicmodeling.preprocessing.ListCorpusCreator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the LDA corpus creation.
 * 
 * @author Michael Roeder, Marleen W.
 *
 */
public class LDACorpusCreation {

	// -------------------------------------------------------------------------
	// ------------------ Variables --------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Logger for errors, warnings and other informations.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(LDACorpusCreation.class);

	/**
	 * Identifier for the LDA corpus file.
	 */
	public static final String CORPUS_NAME = "lodDiagram";

	/**
	 * The LDA corpus file (/path_to_output_folder/lodDiagram.corpus).
	 */
	public static String corpusFile;

	/**
	 * The LDA corpus object
	 * (/path_to_output_folder/lodDiagram_uriUsage_wordOccurence.object).
	 */
	public static String ldaCorpusFile;

	/**
	 * The corpus name. Set according to UriUsage and WordOccurence.
	 */
	protected final String corpusName;

	/**
	 * The UriUsage (classes, properties or all).
	 */
	protected final UriUsage uriUsage;

	/**
	 * The WordOccurence (unique or log).
	 */
	protected final WordOccurence wordOccurence;

	// -------------------------------------------------------------------------
	// ------------------ Methods ----------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Constructor for LDACorpusCreation.
	 * 
	 * @param uriUsage
	 *            the UriUsage
	 * @param wordOccurence
	 *            the WordOccurence
	 */
	public LDACorpusCreation(UriUsage uriUsage, WordOccurence wordOccurence) {
		this.corpusName = CORPUS_NAME;
		this.uriUsage = uriUsage;
		this.wordOccurence = wordOccurence;

	}

	/**
	 * Running the LDACorpusCreation.
	 * 
	 * @param cachingLabelRetriever
	 *            the labelRetriever
	 */
	public void run(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {

		String corpusName = generateCorpusName();

		ldaCorpusFile = IndexGeneratorMain.outputFolder + File.separator + corpusName;

		Corpus corpus = generateCorpusAndIndexWords(cachingLabelRetriever);

		cachingLabelRetriever.storeCache();

		CorpusObjectWriter writer = new GZipCorpusObjectWriter(new File(ldaCorpusFile));
		writer.writeCorpus(corpus);

	}

	/**
	 * Generates tokenized documents based on their URI counts. URIs are
	 * filtered and counted. After that their labels are retrieved and added to
	 * the documents {@link SimpleTokenizedText} based on the
	 * {@link WordOccurence} used.
	 * 
	 * @param supplier
	 * @param cachingLabelRetriever
	 *            the label retriever
	 * @return enriched supplier
	 */
	protected DocumentSupplier generateDocuments(DocumentSupplier supplier,
			WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {

		// Filter URIs
		supplier = filterUris(supplier);

		// Count the URIs
		supplier = new UriCountMappingCreatingDocumentSupplierDecorator(supplier, uriUsage);

		cachingLabelRetriever.setDecoratedDocumentSupplier(supplier);
		supplier = cachingLabelRetriever;

		supplier = new StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator(supplier, wordOccurence);
		return supplier;
	}

	/**
	 * Filters URIs based on the {@link VocabularyBlacklist}. Note: blacklist
	 * needs to be saved at
	 * tapioca.cores/src/main/resources/vocabulary_blacklist.txt
	 * 
	 * @param supplier
	 *            A document supplier.
	 * @return A document supplier.
	 */
	protected DocumentSupplier filterUris(DocumentSupplier supplier) {
		Set<String> blacklist = VocabularyBlacklist.getInstance(
				LDACorpusCreation.class.getClassLoader().getResourceAsStream("resources/vocabulary_blacklist.txt"));

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

	/**
	 * Generates the specific name for the corpus object file depending on which
	 * UriUsage and WordOccurence is used.
	 * 
	 * @return the corpus name
	 */
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

	/**
	 * Generate the corpus and index words.
	 * 
	 * @param cachingLabelRetriever
	 *            label retriever
	 * @return enriched supplier
	 */
	public Corpus generateCorpusAndIndexWords(
			WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {

		DocumentSupplier supplier = generateCorpus(cachingLabelRetriever);

		Vocabulary vocabulary = new SimpleVocabulary();
		supplier = new SimpleWordIndexingSupplierDecorator(supplier, vocabulary);
		supplier = new DocumentWordCountingSupplierDecorator(supplier);

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

	/**
	 * Convert the corpus in the document supplier to a serialized corpus.
	 * 
	 * @param supplier
	 *            The document supplier (corpus).
	 * @return Serialized corpus.
	 */
	protected DocumentSupplier convertToSerializedSupplier(DocumentSupplier supplier) {
		try {
			File temp = File.createTempFile("tempCorpus", ".temp");
			temp.deleteOnExit();

			XmlWritingDocumentConsumer consumer = XmlWritingDocumentConsumer.createXmlWritingDocumentConsumer(temp);
			XmlWritingDocumentConsumer.registerParseableDocumentProperty(DatasetClassInfo.class);
			XmlWritingDocumentConsumer.registerParseableDocumentProperty(DatasetSpecialClassesInfo.class);
			XmlWritingDocumentConsumer.registerParseableDocumentProperty(DatasetPropertyInfo.class);
			XmlWritingDocumentConsumer.registerParseableDocumentProperty(DatasetVocabularies.class);

			Document document = supplier.getNextDocument();
			while (document != null) {
				try {
					consumer.consumeDocument(document);
				} catch (Exception e) {
					LOGGER.error("Exception at document #" + document.getDocumentId() + ". Aborting.", e);
				}
				document = supplier.getNextDocument();
			}
			try {
				consumer.close();
			} catch (IOException e) {
				LOGGER.warn("Got an exception while closing the XML Writer.", e);
			}

			supplier = StreamBasedXmlDocumentSupplier.createReader(temp, true);
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

		} catch (IOException e) {
			LOGGER.error("Error while creating temporary corpus file.", e);
			LOGGER.info("The serialized corpus can not be created, try to work with non-serialized corpus.");
		}
		return supplier;
	}

	/**
	 * Generate the initial corpus.
	 * 
	 * @param cachingLabelRetriever
	 *            The label cache.
	 * @return The initial corpus as document supplier.
	 */
	public DocumentSupplier generateCorpus(WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {
		DocumentSupplier supplier = InitialCorpusCreation.supplier;
		supplier = convertToSerializedSupplier(supplier);
		supplier = generateDocuments(supplier, cachingLabelRetriever);
		supplier = filterStopWordsAndEmptyDocs(supplier);
		return supplier;
	}
}
