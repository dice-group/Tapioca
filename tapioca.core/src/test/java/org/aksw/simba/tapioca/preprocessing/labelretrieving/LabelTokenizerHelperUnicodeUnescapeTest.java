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
public class LabelTokenizerHelperUnicodeUnescapeTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays
                .asList(new Object[][] {
                        {
                                "Activos por sector econ\u00F3mico (CNAE 09)",
                                "Activos por sector econ" + Character.toChars(0xF3)[0] + "mico (CNAE 09)" },
                        {
                                "\u043d\u0430\u0446\u0438\u0439",
                                "" + Character.toChars(0x43d)[0] + Character.toChars(0x430)[0] + Character.toChars(0x446)[0]
                                        + Character.toChars(0x438)[0] + Character.toChars(0x439)[0] }
                });
    }

    private String string;
    private String expectedString;

    public LabelTokenizerHelperUnicodeUnescapeTest(String string, String expectedString) {
        this.string = string;
        this.expectedString = expectedString;
    }

    @Test
    public void test() {
        Assert.assertEquals(expectedString, string);
    }
}
