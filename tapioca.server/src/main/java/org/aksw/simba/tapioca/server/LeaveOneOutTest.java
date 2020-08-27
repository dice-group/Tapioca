package org.aksw.simba.tapioca.server;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.aksw.simba.tapioca.gen.ModelGenerator;
import org.aksw.simba.tapioca.gen.URIBasedIndexGenerator;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.dice_research.topicmodeling.commons.collections.TopDoubleObjectCollection;
import org.dice_research.topicmodeling.io.CorpusReader;
import org.dice_research.topicmodeling.io.gzip.GZipCorpusReaderDecorator;
import org.dice_research.topicmodeling.io.java.CorpusObjectReader;
import org.dice_research.topicmodeling.preprocessing.ListCorpusCreator;
import org.dice_research.topicmodeling.preprocessing.Preprocessor;
import org.dice_research.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.dice_research.topicmodeling.utils.corpus.Corpus;
import org.dice_research.topicmodeling.utils.corpus.DocumentListCorpus;
import org.dice_research.topicmodeling.utils.doc.Document;
import org.dice_research.topicmodeling.utils.doc.DocumentName;
import org.dice_research.topicmodeling.utils.doc.DocumentURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaveOneOutTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(LeaveOneOutTest.class);

	public static void main(String[] args) {
//		Corpus corpus = readBLCorpus("/home/micha/data/sven/sven-corpora/initial.xml");
		Corpus corpus = readTMCorpus("/home/micha/data/sven/sven-corpora/lda-corpus.object");

//		EngineFactory factory = new BLFactory();
		EngineFactory factory = new TMFactory();

		File outputDirectory = new File("/home/micha/data/sven/leave-one-out-output");

		leaveOneOut(corpus, factory, outputDirectory);
	}

	public static void leaveOneOut(Corpus corpus, EngineFactory factory, File outputDirectory) {
		List<Document> trainDocuments;
		Corpus trainCorpus;
		Document testDocument;
		for (int i = 0; i < corpus.getNumberOfDocuments(); ++i) {
			LOGGER.info("Starting test with document {} as held-out document.", i);
			if (i == 0) {
				trainDocuments = new ArrayList<>();
			} else {
				trainDocuments = new ArrayList<>(corpus.getDocuments(0, i));
			}
			if (i < (corpus.getNumberOfDocuments() - 1)) {
				trainDocuments.addAll(corpus.getDocuments(i + 1, corpus.getNumberOfDocuments()));
			}
			trainCorpus = new DocumentListCorpus<List<Document>>(trainDocuments);
			trainCorpus.setProperties(corpus.getProperties());

			testDocument = corpus.getDocument(i);

			File foldDirectory = new File(outputDirectory.getAbsolutePath() + File.separator + Integer.toString(i));
			AbstractEngine engine = factory.create(trainCorpus, foldDirectory);
			TopDoubleObjectCollection<String> result = engine.retrieveSimilarDatasets(testDocument);

			printResult(result, testDocument, factory.getEngineDesc(), outputDirectory);

			trainDocuments = null;
			trainCorpus = null;
			engine = null;
			result = null;
			System.gc();
		}
	}

	private static void printResult(TopDoubleObjectCollection<String> result, Document testDocument, String engineDesc,
			File foldDirectory) {
		File outputFile = new File(foldDirectory.getAbsolutePath() + File.separator + engineDesc + "_result.tsv");
		String name;
		try (PrintStream pout = new PrintStream(new BufferedOutputStream(new FileOutputStream(outputFile, true)))) {
			DocumentName dName = testDocument.getProperty(DocumentName.class);
			if (dName == null) {
				DocumentURI dUri = testDocument.getProperty(DocumentURI.class);
				if (dUri == null) {
					name = Integer.toString(testDocument.getDocumentId());
				} else {
					name = dUri.get();
				}
			} else {
				name = dName.get();
			}
			pout.print(name);

			for (int i = 0; i < result.size(); ++i) {
				pout.print('\t');
				pout.print(result.objects[i]);
				pout.print('\t');
				pout.print(result.values[i]);
			}
			pout.println();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	protected static Corpus readTMCorpus(String inputFile) {
		CorpusReader reader = new GZipCorpusReaderDecorator(new CorpusObjectReader());
		reader.readCorpus(new File(inputFile));
		return reader.getCorpus();
	}

	protected static Corpus readBLCorpus(String inputFile) {
		DocumentSupplier supplier = URIBasedIndexGenerator.createBLPreprocessing(new File(inputFile));

		Preprocessor preprocessor = new ListCorpusCreator<>(supplier,
				new DocumentListCorpus<List<Document>>(new ArrayList<>()));

		return preprocessor.getCorpus();
	}

	public static interface EngineFactory {
		public AbstractEngine create(Corpus trainCorpus, File outputDirectory);

		public String getEngineDesc();
	}

	public static class BLFactory implements EngineFactory {
		@Override
		public AbstractEngine create(Corpus trainCorpus, File outputDirectory) {
			BLEngine engine = BLEngine.createEngine(trainCorpus, null);
			engine.setNumberOfResults(trainCorpus.getNumberOfDocuments());
			return engine;
		}

		@Override
		public String getEngineDesc() {
			return "BL";
		}
	}

	public static class TMFactory implements EngineFactory {
		private int numberOfTopics = 25;
		private WorkerBasedLabelRetrievingDocumentSupplierDecorator decorator = new WorkerBasedLabelRetrievingDocumentSupplierDecorator(
				null, new File[0]);

		@Override
		public AbstractEngine create(Corpus trainCorpus, File outputDirectory) {
			if(!outputDirectory.exists()) {
				outputDirectory.mkdir();
			}
			// Generate model
			String modelObjFile = outputDirectory.getAbsolutePath() + File.separator + TMEngine.MODEL_FILE_NAME;
			ModelGenerator generator = new ModelGenerator(numberOfTopics, 1040);
			generator.run(trainCorpus, modelObjFile);
			// Create TM engine
			TMEngine engine = TMEngine.createEngine(decorator, trainCorpus, outputDirectory, null);
			engine.setNumberOfResults(trainCorpus.getNumberOfDocuments());
			return engine;
		}

		@Override
		public String getEngineDesc() {
			return "Tapioca";
		}
	}
}
