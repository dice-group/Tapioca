package org.aksw.simba.tapioca.indexgenerator.docgen;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.aksw.simba.tapioca.cores.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator;
import org.aksw.simba.tapioca.cores.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.algorithms.ProbTopicModelingAlgorithmStateSupplier;
import org.aksw.simba.topicmodeling.algorithms.WordCounter;
import org.aksw.simba.topicmodeling.utils.vocabulary.Vocabulary;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IndexGeneratorMainTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(IndexGeneratorMainTest.class);

	@Before
	public void setUp() throws Exception {
		FileUtils.deleteDirectory(new File("src/test/data/void_IndexGenerator_Output"));
	}

	@After
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(new File("src/test/data/void_IndexGenerator_Output"));
	}

	/**
	 * Read expected results from input files Note: strings_3.txt contains the
	 * blacklist words, which shouldn't be counted
	 * 
	 * @return Expected results as Map<String, Integer>
	 * @throws IOException
	 */
	private Map<String, Integer> readExpected(boolean withCounts) throws IOException {
		// initialize
		Map<String, Integer> expected = new HashMap<String, Integer>();
		String line;

		// read
		for (int i = 0; i <= 2; i++) {
			BufferedReader br = new BufferedReader(new FileReader("src/test/data/strings_" + i + ".txt"));
			while ((line = br.readLine()) != null) {
				String[] wordToCount = line.split("\t");
				Integer count = Integer.parseInt(wordToCount[1]);
				if (withCounts) {
					count = (int) (count > 1 ? Math.round(Math.log(count)) + 1 : 1);
				} else {
					count = (int) 1;
				}

				expected.put(wordToCount[0], count);
			}
			br.close();
		}

		// return
		return expected;
	}

	private boolean testResult(Map<String, Integer> result, Map<String, Integer> expected, boolean withCounts) {
		// initialize
		boolean isSame = true;

		// loop
		for (Map.Entry<String, Integer> entry : result.entrySet()) {
			String word = entry.getKey();
			// do evaluate counts
			if (withCounts) {
				isSame = expected.containsKey(word) && (expected.get(word) == result.get(word));
			}
			// do not evaluate counts
			else {
				isSame = expected.containsKey(word);
			}
			System.out.println(entry.getKey());
			if (!isSame) {
				System.out.println("Error at key: " + entry.getKey());
				// show error for counts
				if (withCounts) {
					System.out
							.println("result counts: " + result.get(word) + " expected counts: " + expected.get(word));
				}
				break;
			}
		}

		// return
		return isSame;
	}

	/**
	 * Test for too little commandline parameters
	 * 
	 * @throws IOException
	 */
	@Test
	public final void test01Main() throws IOException {
		String[] args = { "src/test/data/void", "2" };
		LOGGER.info("\n#01: Too little parameters");
		IndexGeneratorMain.main(args);
		assertNull(IndexGeneratorMain.inputFolder);
	}

	/**
	 * Test for too many commandline parameters
	 * 
	 * @throws IOException
	 */
	@Test
	public final void test02Main() throws IOException {
		String[] args = { "1", "2", "3", "4", "5", "6" };
		LOGGER.info("\n#02: Too many parameters");
		IndexGeneratorMain.main(args);
		assertNull(IndexGeneratorMain.inputFolder);
	}

	/**
	 * Test for invalid argument for UriUsage
	 * 
	 * @throws IOException
	 */
	@Test
	public final void test03Main() throws IOException {
		String[] args = { "src/test/data/void", "santa", "unique", "10", "LDA" };
		LOGGER.info("\n#03: Invalid argument for uriUsage");
		IndexGeneratorMain.main(args);
		assertNull(UriCountMappingCreatingDocumentSupplierDecorator.getEnum(args[1]));
	}

	/**
	 * Test for invalid Argument for WordOccurence
	 * 
	 * @throws IOException
	 */
	@Test
	public final void test04Main() throws IOException {
		String[] args = { "src/test/data/void", "classes", "clause", "10", "LDA" };
		LOGGER.info("\n#04: Invalid argument for wordOccurence");
		IndexGeneratorMain.main(args);
		assertNull(StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.getEnum(args[2]));

	}

	/**
	 * 
	 * Test method for IndexGeneratorMain#makeDirectory(), checks for invalid
	 * input path
	 */
	@Test
	public void test05MakeDirectory() {
		String[] args = { "ieuiwfhiugfe", "all", "log", "5", "LDA" };
		LOGGER.info("\n#05: Input Folder is not a path, failed to create output folders");
		assertFalse(IndexGeneratorMain.makeDirectory(args[0]));
	}

	/**
	 * Tests corpus creation for ALL UNIQUE
	 * 
	 * @throws IOException
	 */
//	@Test
//	public void test06RunAllUnique() throws IOException {
//
//		LOGGER.info("\n#06: Start test: All, Unique");
//
//		// initialize
//		String[] args = { "src/test/data/void", "all", "unique", "2", "lda" };
//		Map<String, Integer> result = new HashMap<String, Integer>();
//		IndexGeneratorMain.main(args);
//		ProbTopicModelingAlgorithmStateSupplier supplier = (ProbTopicModelingAlgorithmStateSupplier) IndexGeneratorMain.algorithm;
//		Vocabulary vocabulary = supplier.getVocabulary();
//
//		// read expected input
//		Map<String, Integer> expected = readExpected(false);
//
//		// execution
//		for (int i = 0; i < supplier.getNumberOfWords(); i++) {
//			result.put(vocabulary.getWord(i), 1);
//		}
//
//		// test
//		assertTrue(testResult(result, expected, false));
//	}

	/**
	 * Tests corpus creation for ALL LOG We only need the test for All/Log,
	 * because ALL/Unique has the same words with countsOfWord = 1 for every
	 * word.
	 * 
	 * @throws IOException
	 */
	@Test
	public void test07RunAllLog() throws IOException {

		LOGGER.info("\n#06: Start test: All, Log");

		// initialize
		String[] args = { "src/test/data/void", "all", "log", "2", "lda" };
		Map<String, Integer> result = new HashMap<String, Integer>();
		IndexGeneratorMain.main(args);
		ProbTopicModelingAlgorithmStateSupplier supplier = (ProbTopicModelingAlgorithmStateSupplier) IndexGeneratorMain.algorithm;
		Vocabulary vocabulary = supplier.getVocabulary();
		WordCounter wc = supplier.getWordCounts();

		// read expected input
		Map<String, Integer> expected = readExpected(true);

		// execution
		for (int i = 0; i < supplier.getNumberOfWords(); i++) {
			String word = vocabulary.getWord(i);
			result.put(word, wc.getCountOfWord(i));
		}

		// test
		assertTrue(testResult(result, expected, true));
	}

	@Test
	public void test08verifyIndex() throws IOException {
		String[] args = { "src/test/data/void", "all", "log", "2", "LDA" };
		LOGGER.info("\n#7: check index for expected number of Document, number of topics and number of words");
		IndexGeneratorMain.main(args);

		ProbTopicModelingAlgorithmStateSupplier probTopicModeling = (ProbTopicModelingAlgorithmStateSupplier) IndexGeneratorMain.algorithm;
		int numberOfDocuments = probTopicModeling.getNumberOfDocuments();
		int numberOfTopics = probTopicModeling.getNumberOfTopics();
		int numberOfWords = probTopicModeling.getNumberOfWords();

		int expectedDocuments = 3;
		int expectedTopics = 2;
		int expectedWords = 19;

		assertEquals(expectedDocuments, numberOfDocuments);
		assertEquals(expectedTopics, numberOfTopics);
		assertEquals(expectedWords, numberOfWords);
	}
}
