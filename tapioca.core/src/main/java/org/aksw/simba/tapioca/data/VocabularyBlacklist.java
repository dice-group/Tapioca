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
