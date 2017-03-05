/**
 * This file is part of tapioca.cores.
 *
 * tapioca.cores is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.cores is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.cores.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.cores.preprocessing;

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
