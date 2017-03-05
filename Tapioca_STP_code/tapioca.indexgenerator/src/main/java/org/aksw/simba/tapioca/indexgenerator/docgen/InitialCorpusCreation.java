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
import java.util.Arrays;

import org.aksw.simba.tapioca.cores.preprocessing.JenaBasedVoidParsingSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.WordOccurence;
import org.aksw.simba.tapioca.cores.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
import org.aksw.simba.tapioca.cores.preprocessing.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.io.FolderReader;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentTextCreatingSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.PropertyRemovingSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.DocumentRawData;
import org.aksw.simba.topicmodeling.utils.doc.DocumentText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the document generation for the index generator.
 * 
 * @author Michael Roeder, Marleen W.
 */
public class InitialCorpusCreation {

	// -------------------------------------------------------------------------
	// ------------------ Variables --------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Logger for errors, warnings and other informations.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(InitialCorpusCreation.class);

	/**
	 * Identifier for the initial corpus.
	 */
	public static final String CORPUS_NAME = "lodStats";

	/**
	 * The initial corpus file.
	 */
	public static String corpusFile;

	/**
	 * Cache files for labels.
	 */
	public File CACHE_FILES[] = new File[] {
			new File(IndexGeneratorMain.cacheFolder + File.separator + "uriToLabelCache_1.object"),
			new File(IndexGeneratorMain.cacheFolder + File.separator + "uriToLabelCache_2.object"),
			new File(IndexGeneratorMain.cacheFolder + File.separator + "uriToLabelCache_3.object") };

	/**
	 * The document supplier.
	 */
	public static DocumentSupplier supplier;

	// -------------------------------------------------------------------------
	// ------------------ Methods ----------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Run the initial corpus creation.
	 * 
	 * @param inputFolder
	 *            the input folder where the meta data files are stored
	 * @param useUri
	 *            defines which UriUsage to use
	 * @param useWordOccurence
	 *            defines which WordOccurence to use
	 */
	protected void run(File inputFolder, UriUsage uriUsage, WordOccurence wordOccurence) {
		// read in input folder and apply supplier
		FolderReader reader = new FolderReader(inputFolder);
		reader.setUseFolderNameAsCategory(true);
		supplier = reader;
		supplier = new DocumentTextCreatingSupplierDecorator(supplier);
		supplier = new JenaBasedVoidParsingSupplierDecorator(supplier);
		supplier = new PropertyRemovingSupplierDecorator(supplier,
				Arrays.asList(DocumentRawData.class, DocumentText.class));

		File labelsFiles[] = new File[] { new File(LDACorpusCreation.corpusFile.replace(".corpus", ".labels.object")),
				new File(LDACorpusCreation.corpusFile.replace(".corpus", ".ret_labels_1.object")) };

		WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever;
		cachingLabelRetriever = new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, CACHE_FILES, labelsFiles);

		// running LDA corpus creation
		LDACorpusCreation corpusCreation = new LDACorpusCreation(uriUsage, wordOccurence);
		LOGGER.info("Starting corpus \"" + corpusCreation.corpusName + "\" with " + uriUsage + " and " + wordOccurence);
		corpusCreation.run(cachingLabelRetriever);
		cachingLabelRetriever.close();

	}

}
