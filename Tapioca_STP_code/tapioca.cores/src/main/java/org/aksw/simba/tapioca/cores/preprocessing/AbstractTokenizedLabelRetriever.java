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

public abstract class AbstractTokenizedLabelRetriever implements TokenizedLabelRetriever {

    // private static final String TOKENIZE_PATTERN = "[ _]";

    // public static String[] tokenizeLabel(String untokenizedLabel) {
    // String tokens[] = untokenizedLabel.split(TOKENIZE_PATTERN);
    // boolean tokenGood;
    // int count = 0, pos;
    // for (int i = 0; i < tokens.length; ++i) {
    // tokenGood = false;
    // pos = 0;
    // // check if this token is good
    // while ((!tokenGood) && (pos < tokens[i].length())) {
    // tokenGood = Character.isLetter(tokens[i].charAt(pos));
    // ++pos;
    // }
    // if (tokenGood) {
    // // prepare the label
    // tokens[i] = tokens[i].toLowerCase();
    // // check that there are no punctuations at the end
    // pos = tokens[i].length();
    // while ((pos > 0) && (!Character.isLetterOrDigit(tokens[i].charAt(pos - 1)))) {
    // --pos;
    // }
    // if (pos < tokens[i].length()) {
    // tokens[i] = tokens[i].substring(0, pos);
    // }
    // ++count;
    // } else {
    // tokens[i] = null;
    // }
    // }
    // if (count == tokens.length) {
    // return tokens;
    // } else {
    // String cleanedTokens[] = new String[count];
    // pos = 0;
    // for (int i = 0; i < tokens.length; ++i) {
    // if (tokens[i] != null) {
    // cleanedTokens[pos] = tokens[i];
    // ++pos;
    // }
    // }
    // return cleanedTokens;
    // }
    // }
}
