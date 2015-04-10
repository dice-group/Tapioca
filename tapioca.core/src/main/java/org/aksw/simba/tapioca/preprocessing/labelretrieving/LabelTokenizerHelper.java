package org.aksw.simba.tapioca.preprocessing.labelretrieving;

import java.util.ArrayList;
import java.util.List;

public class LabelTokenizerHelper {

    /*
     * Internal Automaton states
     */
    private static final int SAW_SYMBOL_OR_DIGIT_CHAR = 0;
    private static final int SAW_LOWERCASE_CHAR = 1;
    private static final int SAW_UPPERCASE_CHAR = 2;

    public static List<String> getSeparatedText(String text) {
        List<String> tokens = new ArrayList<String>();
        char chars[] = text.toCharArray();
        int state = SAW_SYMBOL_OR_DIGIT_CHAR;
        int start = 0;
        for (int i = 0; i < chars.length; ++i) {
            if (Character.isLetter(chars[i])) {
                if (Character.isUpperCase(chars[i])) {
                    // if this label is written with CamelCase
                    if (state == SAW_LOWERCASE_CHAR) {
                        tokens.add(text.substring(start, i).toLowerCase());
                        start = i;
                    }
                    state = SAW_UPPERCASE_CHAR;
                } else {
                    state = SAW_LOWERCASE_CHAR;
                }
            } else {
                if (state != SAW_SYMBOL_OR_DIGIT_CHAR) {
                    tokens.add(text.substring(start, i).toLowerCase());
                    state = SAW_SYMBOL_OR_DIGIT_CHAR;
                }
                start = i + 1;
            }
        }
        if (start < chars.length) {
            tokens.add(text.substring(start).toLowerCase());
        }
        return tokens;
    }

    /**
     * Unescapes \\uXXXX encoded chars.
     */
    public static String unescapeUnicodeChars(String string) {
        int pos = string.indexOf("\\u");
        if (pos < 0) {
            return string;
        }
        StringBuilder newString = new StringBuilder();
        int charValue, oldPos = 0;
        while ((pos >= 0) && (pos + 5 < string.length())) {
            newString.append(string.substring(oldPos, pos));
            try {
                charValue = Integer.parseInt(string.substring(pos + 2, pos + 6), 16);
                newString.append(Character.toChars(charValue));
                oldPos = pos + 6;
            } catch (Exception e) {
                // couldn't extract a number --> seems that the \\u is no encoded unicode
                oldPos = pos + 2;
            }
            pos = string.indexOf("\\u", pos + 2);
        }
        return newString.toString();
    }
}
