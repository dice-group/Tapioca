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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.dice_research.topicmodeling.commons.io.StorageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;

//@ThreadSafe
public class FileBasedTokenizedLabelRetriever extends AbstractTokenizedLabelRetriever {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedTokenizedLabelRetriever.class);

    @Deprecated
    private static final String DEFAULT_FILE = "uriToLabelmapping.object";

    @Deprecated
    public static FileBasedTokenizedLabelRetriever create() {
        return create(DEFAULT_FILE);
    }

    public static FileBasedTokenizedLabelRetriever create(String file) {
        return create(file, false);
    }

    public static FileBasedTokenizedLabelRetriever create(String file, boolean alreadyTokenized) {
        String uriToLabel[][] = StorageHelper.readFromFileSavely(file);
        if (uriToLabel == null) {
            LOGGER.error("Couldn't load labels from file. Returning null.");
            return null;
        }
        ObjectObjectOpenHashMap<String, String[]> tokenizedLabels = new ObjectObjectOpenHashMap<String, String[]>(
                uriToLabel[0].length);
        if (alreadyTokenized) {
            String tokens[];
            for (int i = 0; i < uriToLabel.length; ++i) {
                if (uriToLabel[i].length > 1) {
                    tokens = new String[uriToLabel[i].length - 1];
                    System.arraycopy(uriToLabel[i], 1, tokens, 0, tokens.length);
                    tokenizedLabels.put(uriToLabel[i][0], tokens);
                } else {
                    tokenizedLabels.put(uriToLabel[i][0], null);
                }
            }
        } else {
            Set<String> tokens = new HashSet<String>();
            for (int i = 0; i < uriToLabel.length; ++i) {
                for (int j = 1; j < uriToLabel[i].length; ++j) {
                    tokens.addAll(LabelTokenizerHelper.getSeparatedText(uriToLabel[i][j]));
                }
                if (tokens.size() > 0) {
                    tokenizedLabels.put(uriToLabel[i][0], tokens.toArray(new String[tokens.size()]));
                    tokens.clear();
                } else {
                    tokenizedLabels.put(uriToLabel[i][0], null);
                }
            }
        }
        return new FileBasedTokenizedLabelRetriever(tokenizedLabels);
    }

    private ObjectObjectOpenHashMap<String, String[]> tokenizedLabels;

    private FileBasedTokenizedLabelRetriever(ObjectObjectOpenHashMap<String, String[]> tokenizedLabels) {
        this.tokenizedLabels = tokenizedLabels;
    }

    public List<String> getTokenizedLabel(String uri, String namespace) {
        if (tokenizedLabels.containsKey(uri)) {
            String tokens[] = tokenizedLabels.get(uri);
            if (tokens != null) {
                return Arrays.asList();
            }
        }
        return null;
    }

}
