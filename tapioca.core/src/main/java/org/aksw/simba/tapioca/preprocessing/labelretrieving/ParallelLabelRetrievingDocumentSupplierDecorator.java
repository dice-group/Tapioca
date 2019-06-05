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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.aksw.simba.tapioca.data.StringCountMapping;
import org.dice_research.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyEditingDocumentSupplierDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;

public class ParallelLabelRetrievingDocumentSupplierDecorator
        extends AbstractPropertyEditingDocumentSupplierDecorator<StringCountMapping> implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelLabelRetrievingDocumentSupplierDecorator.class);

    // private static final long MAXIMUM_WAITING_TIME = 240000;

    private LocalLabelTokenizer localTokenizer = new LocalLabelTokenizer();
    private TokenizedLabelRetriever localLabelTokenizers[];
    private ThreadSafeCachingLabelTokenizerDecorator clientLabelTokenizer;

    public ParallelLabelRetrievingDocumentSupplierDecorator(DocumentSupplier documentSource, File chacheFiles[]) {
        super(documentSource, StringCountMapping.class);
        // fileBasedLabelTokenizer = FileBasedTokenizedLabelRetriever.create();
        clientLabelTokenizer = ThreadSafeCachingLabelTokenizerDecorator.create(new RDFClientLabelRetriever(),
                chacheFiles);
        localLabelTokenizers = new FileBasedTokenizedLabelRetriever[0];
        // new WaitingThreadInterrupter(overseer, MAXIMUM_WAITING_TIME);
    }

    public ParallelLabelRetrievingDocumentSupplierDecorator(DocumentSupplier documentSource, File chacheFiles[],
            File... labelFiles) {
        this(documentSource, chacheFiles, createFileBasedTokenizers(labelFiles));
    }

    public ParallelLabelRetrievingDocumentSupplierDecorator(DocumentSupplier documentSource, File chacheFiles[],
            List<TokenizedLabelRetriever> localLabelTokenizers) {
        this(documentSource, chacheFiles,
                localLabelTokenizers.toArray(new TokenizedLabelRetriever[localLabelTokenizers.size()]));
    }

    public ParallelLabelRetrievingDocumentSupplierDecorator(DocumentSupplier documentSource, File chacheFiles[],
            TokenizedLabelRetriever localLabelTokenizers[]) {
        super(documentSource, StringCountMapping.class);
        this.localLabelTokenizers = localLabelTokenizers;
        clientLabelTokenizer = ThreadSafeCachingLabelTokenizerDecorator.create(new RDFClientLabelRetriever(),
                chacheFiles);
        // new WaitingThreadInterrupter(overseer, MAXIMUM_WAITING_TIME);
    }

    @Override
    protected void editDocumentProperty(StringCountMapping mapping) {
        ObjectLongOpenHashMap<String> countedUris = mapping.get();
        ObjectLongOpenHashMap<String> countedTokens = new ObjectLongOpenHashMap<String>();
        IntStream.range(0, countedUris.allocated.length).filter(i -> countedUris.allocated[i])
                .forEach(i -> retrieveLabels((String) ((Object[]) countedUris.keys)[i], countedUris.values[i],
                        localTokenizer, localLabelTokenizers, clientLabelTokenizer, countedTokens));
        mapping.set(countedTokens);
    }

    protected static void retrieveLabels(String uri, long count, LocalLabelTokenizer localTokenizer,
            TokenizedLabelRetriever localLabelTokenizers[],
            ThreadSafeCachingLabelTokenizerDecorator clientLabelTokenizer,
            ObjectLongOpenHashMap<String> countedTokens) {
        RDFClientLabelRetriever rdfClient = new RDFClientLabelRetriever();
        // extract namespace
        String namespace = extractVocabulary(uri);

        // Get the tokens of the label
        List<String> tokens = null;
        for (int i = 0; (tokens == null) && (i < localLabelTokenizers.length); ++i) {
            tokens = localLabelTokenizers[i].getTokenizedLabel(uri, namespace);
        }
        // If the label couldn't be retrieved
        if (tokens == null) {
            tokens = clientLabelTokenizer.getTokenizedLabel(rdfClient, uri, namespace);
        }
        // If the label couldn't be retrieved, create it based on the URI
        if ((tokens == null) || (tokens.size() == 0)) {
            tokens = localTokenizer.getTokenizedLabel(uri, namespace);
        }

        synchronized (countedTokens) {
            for (String token : tokens) {
                countedTokens.putOrAdd(token, count, count);
            }
        }
    }

    protected static String extractVocabulary(String uri) {
        String namespace = null;
        // If there is no correct vocab extract it using '/' and '#'
        if (namespace == null) {
            int posSlash, posHash;
            posSlash = uri.lastIndexOf('/');
            posHash = uri.lastIndexOf('#');
            if ((posSlash < 0) && (posHash < 0)) {
                int posColon = uri.lastIndexOf(':');
                if (posColon > 0) {
                    namespace = uri.substring(0, posColon);
                } else {
                    namespace = uri;
                }
            } else {
                int min, max;
                if (posSlash < posHash) {
                    min = posSlash;
                    max = posHash;
                } else {
                    min = posHash;
                    max = posSlash;
                }
                if (max < (uri.length() - 1)) {
                    namespace = uri.substring(0, max);
                } else {
                    if (min > 0) {
                        namespace = uri.substring(0, min);
                    } else {
                        namespace = uri;
                    }
                }
            }
        }
        return namespace;
    }

    public void storeCache() {
        clientLabelTokenizer.storeCache();
    }

    private static List<TokenizedLabelRetriever> createFileBasedTokenizers(File[] labelFiles) {
        List<TokenizedLabelRetriever> tempRetrievers = new ArrayList<>();
        TokenizedLabelRetriever tempRetriever;
        for (int i = 0; i < labelFiles.length; ++i) {
            tempRetriever = FileBasedTokenizedLabelRetriever.create(labelFiles[i].getAbsolutePath());
            if (tempRetriever == null) {
                LOGGER.warn("Couldn't load labels from {}.", labelFiles[i]);
            } else {
                tempRetrievers.add(tempRetriever);
            }
        }
        return tempRetrievers;
    }

    public static class ExceptionThrowingRetriever implements TokenizedLabelRetriever {

        @Override
        public List<String> getTokenizedLabel(String uri, String namespace) {
            throw new IllegalArgumentException();
        }

    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }

}