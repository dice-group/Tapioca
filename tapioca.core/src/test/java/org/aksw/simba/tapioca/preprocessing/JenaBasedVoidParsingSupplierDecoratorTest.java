package org.aksw.simba.tapioca.preprocessing;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.dice_research.topicmodeling.preprocessing.docsupplier.CorpusWrappingDocumentSupplier;
import org.dice_research.topicmodeling.utils.corpus.DocumentListCorpus;
import org.dice_research.topicmodeling.utils.doc.Document;
import org.dice_research.topicmodeling.utils.doc.DocumentText;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;

@RunWith(Parameterized.class)
public class JenaBasedVoidParsingSupplierDecoratorTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { "org/aksw/simba/tapioca/preprocessing/void1.ttl",
                new String[] { "http://example.org/Class1", "http://example.org/Class2", "http://example.org/Class3" },
                new int[] { 1, 22, 333 }, new String[] { "http://example.org/property1", "http://example.org/property2",
                        "http://example.org/property3" },
                new int[] { 10, 200, 3000 } } });
    }

    protected String resource;
    protected String[] classUris;
    protected int[] classCounts;
    protected String[] propertyUris;
    protected int[] propertyCounts;

    public JenaBasedVoidParsingSupplierDecoratorTest(String resource, String[] classUris, int[] classCounts,
            String[] propertyUris, int[] propertyCounts) {
        this.resource = resource;
        this.classUris = classUris;
        this.classCounts = classCounts;
        this.propertyUris = propertyUris;
        this.propertyCounts = propertyCounts;
    }

    @Test
    public void test() throws Exception {
        Document document = new Document(0);
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(resource)) {
            Assert.assertNotNull(is);
            document.addProperty(new DocumentText(IOUtils.toString(is)));
        } catch (Exception e) {
            throw e;
        }

        JenaBasedVoidParsingSupplierDecorator decorator = new JenaBasedVoidParsingSupplierDecorator(
                new CorpusWrappingDocumentSupplier(
                        new DocumentListCorpus<List<Document>>(new ArrayList<Document>(Arrays.asList(document)))));
        document = decorator.getNextDocument();

        ObjectLongOpenHashMap<String> counts;

        DatasetClassInfo classInfo = document.getProperty(DatasetClassInfo.class);
        Assert.assertNotNull(classInfo);
        counts = classInfo.get();
        for (int i = 0; i < classUris.length; ++i) {
            Assert.assertTrue(classUris[i], counts.containsKey(classUris[i]));
            Assert.assertEquals(classUris[i], classCounts[i], counts.get(classUris[i]));
        }
        Assert.assertEquals("More classes where extracted than expected", classCounts.length, counts.size());

        DatasetPropertyInfo propInfo = document.getProperty(DatasetPropertyInfo.class);
        Assert.assertNotNull(propInfo);
        counts = propInfo.get();
        for (int i = 0; i < propertyUris.length; ++i) {
            Assert.assertTrue(propertyUris[i], counts.containsKey(propertyUris[i]));
            Assert.assertEquals(propertyUris[i], propertyCounts[i], counts.get(propertyUris[i]));
        }
        Assert.assertEquals("More properties where extracted than expected", propertyCounts.length, counts.size());
    }
}
