/**
 * This file is part of tapioca.server.
 *
 * tapioca.server is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.server is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.server.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.server;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import junit.framework.Assert;

import org.aksw.simba.tapioca.preprocessing.labelretrieving.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.preprocessing.SingleDocumentPreprocessor;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentText;
import org.aksw.simba.topicmodeling.utils.doc.DocumentWordCounts;
import org.aksw.simba.topicmodeling.utils.vocabulary.SimpleVocabulary;
import org.aksw.simba.topicmodeling.utils.vocabulary.Vocabulary;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PreprocessingTest {

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> testConfigs = new ArrayList<Object[]>();
        testConfigs.add(new Object[] { "test_dataset_1.ttl",
                new String[] { "first", "second", "third", "class", "property" }, new int[] { 2, 1, 1, 2, 1 } });
        testConfigs.add(new Object[] { "test_dataset_2.ttl", null, null });
        return testConfigs;
    }

    private String file;
    private String[] expectedWords;
    private int[] expectedCounts;

    public PreprocessingTest(String file, String[] expectedWords, int[] expectedCounts) {
        this.file = file;
        this.expectedWords = expectedWords;
        this.expectedCounts = expectedCounts;
    }

    @Test
    public void test() throws IOException {
        Vocabulary vocabulary = new SimpleVocabulary();
        SingleDocumentPreprocessor preprocessor = TMEngine.createPreprocessing(
                new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, new File[] { File.createTempFile("test_",
                        "") }), vocabulary);

        String voidString = readVoidString(file);
        Document document = new Document();
        document.addProperty(new DocumentText(voidString));
        document = preprocessor.processDocument(document);
        if (expectedWords == null) {
            Assert.assertNull(document);
            return;
        }
        Assert.assertNotNull(document);
        System.out.println(document.toString());

        DocumentWordCounts wordCounts = document.getProperty(DocumentWordCounts.class);
        Assert.assertNotNull(wordCounts);
        int wordId;
        for (int i = 0; i < expectedWords.length; ++i) {
            wordId = vocabulary.getId(expectedWords[i]);
            Assert.assertTrue(
                    "The expected word \"" + expectedWords[i] + "\" couldn't be found inside the vocabulary.",
                    wordId != Vocabulary.WORD_NOT_FOUND);
            Assert.assertEquals("Counts (" + wordCounts.getCountForWord(wordId) + ") for word " + expectedWords[i]
                    + " do not equal the expected counts (" + expectedCounts[i] + ").", expectedCounts[i],
                    wordCounts.getCountForWord(wordId));
        }
    }

    private String readVoidString(String file) throws IOException {
        InputStream is = null;
        try {
            is = this.getClass().getClassLoader().getResourceAsStream(file);
            Assert.assertNotNull(is);
            return IOUtils.toString(is);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }
}
