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

import java.net.URLDecoder;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalLabelTokenizer implements TokenizedLabelRetriever {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalLabelTokenizer.class);

    // /*
    // * Internal Automaton states
    // */
    // private static final int SAW_SYMBOL_OR_DIGIT_CHAR = 0;
    // private static final int SAW_LOWERCASE_CHAR = 1;
    // private static final int SAW_UPPERCASE_CHAR = 2;

    public List<String> getTokenizedLabel(String uri, String namespace) {
        if (uri.startsWith(namespace)) {
            uri = uri.substring(namespace.length());
        }
        try {
            uri = URLDecoder.decode(uri, "UTF-8");
        } catch (Exception e) {
            LOGGER.warn("Couldn't decode the given URI \"" + uri + "\".", e);
        }
        return LabelTokenizerHelper.getSeparatedText(uri);
    }

    // private List<String> getSeparatedText(String text) {
    // List<String> tokens = new ArrayList<String>();
    // char chars[] = text.toCharArray();
    // int state = SAW_SYMBOL_OR_DIGIT_CHAR;
    // int start = 0;
    // for (int i = 0; i < chars.length; ++i) {
    // if (Character.isLetter(chars[i])) {
    // if (Character.isUpperCase(chars[i])) {
    // // if this label is written with CamelCase
    // if (state == SAW_LOWERCASE_CHAR) {
    // tokens.add(text.substring(start, i));
    // start = i;
    // }
    // state = SAW_UPPERCASE_CHAR;
    // } else {
    // state = SAW_LOWERCASE_CHAR;
    // }
    // } else {
    // if (state != SAW_SYMBOL_OR_DIGIT_CHAR) {
    // tokens.add(text.substring(start, i));
    // state = SAW_SYMBOL_OR_DIGIT_CHAR;
    // }
    // start = i + 1;
    // }
    // }
    // if (start < chars.length) {
    // tokens.add(text.substring(start));
    // }
    // return tokens;
    // }

}
