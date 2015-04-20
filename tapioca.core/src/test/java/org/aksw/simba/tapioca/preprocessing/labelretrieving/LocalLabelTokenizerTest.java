package org.aksw.simba.tapioca.preprocessing.labelretrieving;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class LocalLabelTokenizerTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { "http://www.ifomis.org/bfo/1.1/snap#MaterialEntity",
                "http://www.ifomis.org/bfo/1.1/snap", new String[] { "material", "entity" } },
                { "http://www.ifomis.org/bfo/1.1/snap#IndependentContinuant",
                        "http://www.ifomis.org/bfo/1.1/snap", new String[] { "independent", "continuant" } },
                { "http://www.ifomis.org/bfo/1.1#Entity",
                        "http://www.ifomis.org/bfo/1.1", new String[] { "entity" } },
                { "http://www.w3.org/2002/07/owl#Thing",
                        "http://www.w3.org/2002/07/owl", new String[] { "thing" } },
                { "http://purl.obolibrary.org/obo/ERO_0000020",
                        "http://purl.obolibrary.org/obo", new String[] { "ero" } },
                { "http://eagle-i.org/ont/repo/1.0/hasWorkflowState",
                        "http://eagle-i.org/ont/repo/1.0/", new String[] { "has", "workflow", "state" } },
                { "http://eagle-i.org/ont/repo/1.0/",
                        "http://", new String[] { "eagle", "i", "org", "ont", "repo" } },
                { "http://www.pokepedia.fr/index.php/Sp%C3%A9cial",
                        "http://", new String[] { "www", "pokepedia", "fr", "index", "php", "spécial" } } });
    }

    private String uri;
    private String namespace;
    private String expectedTokens[];

    public LocalLabelTokenizerTest(String uri, String namespace, String[] expectedTokens) {
        this.uri = uri;
        this.namespace = namespace;
        this.expectedTokens = expectedTokens;
    }

    @Test
    public void test() {
        LocalLabelTokenizer tokenizer = new LocalLabelTokenizer();
        List<String> tokens = tokenizer.getTokenizedLabel(uri, namespace);
        Assert.assertArrayEquals(expectedTokens, tokens.toArray(new String[tokens.size()]));
    }
}
