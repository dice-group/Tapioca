package org.aksw.simba.tapioca.preprocessing.labelretrieving;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.aksw.simba.topicmodeling.commons.io.StorageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;

public class ThreadSafeCachingLabelTokenizerDecorator implements TokenizedLabelRetrieverDecorator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadSafeCachingLabelTokenizerDecorator.class);

    public static final File DEFAULT_FILES[] = { new File("uriToLabelCache_1.object"),
            new File("uriToLabelCache_2.object"), new File("uriToLabelCache_3.object") };
    private static final int MAX_CONCURRENT_READERS = 1000;

    private File cacheFiles[];
    private ObjectObjectOpenHashMap<String, String[]> cachedTokenizedLabels;
    private TokenizedLabelRetriever decoratedRetriever;
    private int cacheChanges = 0;
    private int forceStorageAfterChanges = 1000;
    private Semaphore cacheReadMutex = new Semaphore(MAX_CONCURRENT_READERS);
    private Semaphore cacheMutex = new Semaphore(1);
    private boolean requestEntitiesNotFound;

    private ThreadSafeCachingLabelTokenizerDecorator(TokenizedLabelRetriever decoratedRetriever,
            ObjectObjectOpenHashMap<String, String[]> tokenizedLabels, File cacheFiles[]) {
        this(decoratedRetriever, tokenizedLabels, cacheFiles, false);
    }

    private ThreadSafeCachingLabelTokenizerDecorator(TokenizedLabelRetriever decoratedRetriever,
            ObjectObjectOpenHashMap<String, String[]> tokenizedLabels, File cacheFiles[],
            boolean requestEntitiesNotFound) {
        this.decoratedRetriever = decoratedRetriever;
        this.cachedTokenizedLabels = tokenizedLabels;
        this.cacheFiles = cacheFiles;
        this.requestEntitiesNotFound = requestEntitiesNotFound;
    }

    public static ThreadSafeCachingLabelTokenizerDecorator create(TokenizedLabelRetriever decoratedRetriever,
            File files[]) {
        String uriToLabel[][] = loadCache(files);
        if (uriToLabel == null) {
            LOGGER.error("Couldn't load labels from file. Creating empty cache.");
            uriToLabel = new String[0][0];
        }
        ObjectObjectOpenHashMap<String, String[]> tokenizedLabels = new ObjectObjectOpenHashMap<String, String[]>(
                uriToLabel.length);
        String tokenizedLabel[];
        for (int i = 0; i < uriToLabel.length; ++i) {
            if (uriToLabel[i].length > 1) {
                tokenizedLabel = new String[uriToLabel[i].length - 1];
                System.arraycopy(uriToLabel[i], 1, tokenizedLabel, 0, tokenizedLabel.length);
            } else {
                tokenizedLabel = null;
            }
            tokenizedLabels.put(uriToLabel[i][0], tokenizedLabel);
        }
        return new ThreadSafeCachingLabelTokenizerDecorator(decoratedRetriever, tokenizedLabels, files);
    }

    private static String[][] loadCache(File[] files) {
        String uriToLabel[][];
        for (int i = 0; i < files.length; ++i) {
            uriToLabel = StorageHelper.readFromFileSavely(files[i].getAbsolutePath());
            if (uriToLabel != null) {
                return uriToLabel;
            }
        }
        return null;
    }

    @Override
    public List<String> getTokenizedLabel(String uri, String namespace) {
        return getTokenizedLabel( decoratedRetriever, uri, namespace);
    }

    public List<String> getTokenizedLabel(TokenizedLabelRetriever decoratedRetriever, String uri, String namespace) {
        List<String> result = null;
        try {
            cacheReadMutex.acquire();
        } catch (InterruptedException e) {
            LOGGER.error("Exception while waiting for read mutex. Returning null.", e);
            return null;
        }
        boolean uriIsCached = cachedTokenizedLabels.containsKey(uri);
        if (uriIsCached) {
            String cachedResult[] = cachedTokenizedLabels.get(uri);
            if (cachedResult != null) {
                result = Arrays.asList(cachedResult);
            }
        }
        // If the URI is not in the cache, or it has been cached but the result is null and the request should be
        // retried
        if (!uriIsCached || (uriIsCached && (result == null) && requestEntitiesNotFound)) {
            cacheReadMutex.release();
            result = decoratedRetriever.getTokenizedLabel(uri, namespace);
            cacheReadMutex.release();
            try {
                cacheMutex.acquire();
                // now we need all others
                cacheReadMutex.acquire(MAX_CONCURRENT_READERS);
            } catch (InterruptedException e) {
                LOGGER.error("Exception while waiting for read mutex. Returning null.", e);
                return null;
            }
            cachedTokenizedLabels.put(uri, result != null ? result.toArray(new String[result.size()]) : null);
            ++cacheChanges;
            if ((forceStorageAfterChanges > 0) && (cacheChanges >= forceStorageAfterChanges)) {
                LOGGER.info("Storing the cache has been forced...");
                performCacheStorage();
            }
            // The last one will be released at the end
            cacheReadMutex.release(MAX_CONCURRENT_READERS - 1);
            cacheMutex.release();
        }
        cacheReadMutex.release();
        return result;
    }

    @Override
    public TokenizedLabelRetriever getDecoratedLabelRetriever() {
        return decoratedRetriever;
    }

    public void storeCache() {
        try {
            cacheMutex.acquire();
        } catch (InterruptedException e) {
            LOGGER.error("Exception while waiting for write mutex for storing the cache. Aborting.", e);
            return;
        }
        performCacheStorage();
        cacheMutex.release();
    }

    private void performCacheStorage() {
        for (int i = cacheFiles.length - 2; i >= 0; --i) {
            cacheFiles[i].renameTo(cacheFiles[i + 1]);
        }
        String uriToLabel[][] = new String[cachedTokenizedLabels.size()][];
        int pos = 0;
        String tokenizedLabel[];
        for (int i = 0; i < cachedTokenizedLabels.allocated.length; ++i) {
            if (cachedTokenizedLabels.allocated[i]) {
                tokenizedLabel = (String[]) ((Object[]) cachedTokenizedLabels.values)[i];
                if (tokenizedLabel != null) {
                    uriToLabel[pos] = new String[tokenizedLabel.length + 1];
                    System.arraycopy(tokenizedLabel, 0, uriToLabel[pos], 1, tokenizedLabel.length);
                } else {
                    uriToLabel[pos] = new String[1];
                }
                uriToLabel[pos][0] = (String) ((Object[]) cachedTokenizedLabels.keys)[i];
                ++pos;
            }
        }
        StorageHelper.storeToFileSavely(uriToLabel, cacheFiles[0].getAbsolutePath());
        cacheChanges = 0;
    }
}
