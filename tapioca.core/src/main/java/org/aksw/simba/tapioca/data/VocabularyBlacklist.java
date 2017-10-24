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
/**
 * This file is part of tapioca.core.
 *
 * tapioca.core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.core.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VocabularyBlacklist {

    private static final Logger LOGGER = LoggerFactory.getLogger(VocabularyBlacklist.class);

    private static final String VOCABULARY_BLACKLIST_FILE = "vocabulary_blacklist.txt";

    private static Set<String> instance = null;

    public static Set<String> getInstance() {
        if (instance == null) {
            synchronized (LOGGER) {
                if (instance == null) {
                    instance = loadList(VOCABULARY_BLACKLIST_FILE);
                }
            }
        }
        return instance;
    }

    protected static Set<String> loadList(String listName) {
        InputStream is = VocabularyBlacklist.class.getClassLoader().getResourceAsStream(listName);
        if (is == null) {
            LOGGER.error("Couldn't load list from resources. Returning null.");
            return new HashSet<String>();
        }
        Set<String> list = null;
        try {
            List<String> lines = IOUtils.readLines(is);
            list = new HashSet<String>(lines);
        } catch (IOException e) {
            LOGGER.error("Couldn't load list from resources. Returning null.", e);
        } finally {
            IOUtils.closeQuietly(is);
        }
        return list;
    }
}
