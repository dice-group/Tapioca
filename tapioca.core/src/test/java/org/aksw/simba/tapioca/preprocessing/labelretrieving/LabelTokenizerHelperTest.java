/**
 * tapioca.core - ${project.description}
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
