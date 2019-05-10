package org.aksw.simba.tapioca.gen;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.aksw.simba.topicmodeling.io.xml.stream.StreamBasedXmlDocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.aksw.simba.topicmodeling.utils.doc.DocumentURI;
import org.junit.Assert;
import org.junit.Test;

public class LaundromatCorpusUpdaterTest {

    @Test
    public void test() throws IOException {
        File inputFile = new File("src/test/resources/corpus.xml");
        File tsvFile = new File("src/test/resources/laundromat.tsv");
        File outputFile = File.createTempFile("test", ".xml");
        LaundromatCorpusUpdater updater = new LaundromatCorpusUpdater();
        updater.run(tsvFile, inputFile, outputFile);

        Set<String> expectedUris = new HashSet<String>(Arrays.asList(
                "http://www.icane.es/data/api/active-population-economic-sector-nace09.rdf", "models/4.06.rdf",
                "data/worldbank-linkeddata/data/climates/433.2046.2065.annualavg.ppt_days10.rdf"));

        DocumentSupplier supplier = StreamBasedXmlDocumentSupplier.createReader(outputFile);
        Document document;
        DocumentURI uri;
        while ((document = supplier.getNextDocument()) != null) {
            uri = document.getProperty(DocumentURI.class);
            Assert.assertNotNull(uri);
            Assert.assertNotNull(uri.get());
            Assert.assertTrue("Found an unexpected URI " + uri.getValue(), expectedUris.remove(uri.getValue()));
        }
        Assert.assertEquals(
                "Didn't found all expected URIs. The following URIs are missing: " + expectedUris.toString(), 0,
                expectedUris.size());
    }
}
