package org.aksw.simba.tapioca.preprocessing.labelretrieving;

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
