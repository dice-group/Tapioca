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
