package org.aksw.simba.tapioca.preprocessing.labelretrieving;

import java.util.Arrays;
import java.util.Collection;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class LabelTokenizerHelperTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays
                .asList(new Object[][] {
                        {
                                "The Registry! :: RDA Group 1 Elements :: Show Detail",
                                new String[] { "the", "registry", "rda", "group", "elements", "show", "detail" } } });
    }

    private String label;
    private String expectedTokens[];

    public LabelTokenizerHelperTest(String label, String[] expectedTokens) {
        this.label = label;
        this.expectedTokens = expectedTokens;
    }

    @Test
    public void test() {
        // String tokens[] = FileBasedLabelTokenizer.tokenizeLabel(label);
        String tokens[] = LabelTokenizerHelper.getSeparatedText(label).toArray(new String[0]);
        Assert.assertEquals(expectedTokens.length, tokens.length);
        for (int i = 0; i < expectedTokens.length; ++i) {
            Assert.assertEquals(expectedTokens[i], tokens[i]);
        }
    }
}
