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
import java.util.concurrent.Semaphore;

import org.aksw.simba.tapioca.data.StringCountMapping;
import org.dice_research.topicmodeling.concurrent.overseers.pool.DefeatableOverseer;
import org.dice_research.topicmodeling.concurrent.overseers.pool.ExecutorBasedOverseer;
import org.dice_research.topicmodeling.concurrent.reporter.LogReporter;
import org.dice_research.topicmodeling.concurrent.reporter.Reporter;
import org.dice_research.topicmodeling.concurrent.tasks.waiting.AbstractWaitingTask;
import org.dice_research.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.dice_research.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyEditingDocumentSupplierDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;

public class WorkerBasedLabelRetrievingDocumentSupplierDecorator
        extends AbstractPropertyEditingDocumentSupplierDecorator<StringCountMapping> implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(WorkerBasedLabelRetrievingDocumentSupplierDecorator.class);

    // private static final int MAX_NUMBER_OF_WORKERS = 20;
    private static final int MAX_NUMBER_OF_WORKERS = 3;

    // private static final long MAXIMUM_WAITING_TIME = 240000;

    private LocalLabelTokenizer localTokenizer = new LocalLabelTokenizer();
    private TokenizedLabelRetriever localLabelTokenizers[];
    private ThreadSafeCachingLabelTokenizerDecorator clientLabelTokenizer;
    private final Semaphore activeWorkers;
    private ObjectLongOpenHashMap<String> countedTokens;
    private final DefeatableOverseer overseer;
    @SuppressWarnings("unused")
    private final Reporter reporter;

    public WorkerBasedLabelRetrievingDocumentSupplierDecorator(DocumentSupplier documentSource, File chacheFiles[],
            TokenizedLabelRetriever localLabelTokenizers[], int numberOfWorkers) {
        super(documentSource, StringCountMapping.class);
        activeWorkers = new Semaphore(numberOfWorkers);
        overseer = new ExecutorBasedOverseer(numberOfWorkers);
        reporter = new LogReporter(overseer);
        // fileBasedLabelTokenizer = FileBasedTokenizedLabelRetriever.create();
        clientLabelTokenizer = ThreadSafeCachingLabelTokenizerDecorator.create(new RDFClientLabelRetriever(),
                chacheFiles);
        this.localLabelTokenizers = localLabelTokenizers;
        // new WaitingThreadInterrupter(overseer, MAXIMUM_WAITING_TIME);
    }

    public WorkerBasedLabelRetrievingDocumentSupplierDecorator(DocumentSupplier documentSource, File chacheFiles[],
            TokenizedLabelRetriever localLabelTokenizers[]) {
        this(documentSource, chacheFiles, localLabelTokenizers, MAX_NUMBER_OF_WORKERS);
    }

    public WorkerBasedLabelRetrievingDocumentSupplierDecorator(DocumentSupplier documentSource, File chacheFiles[],
            File... labelFiles) {
        this(documentSource, chacheFiles, createFileBasedTokenizers(labelFiles), MAX_NUMBER_OF_WORKERS);
    }

    public WorkerBasedLabelRetrievingDocumentSupplierDecorator(DocumentSupplier documentSource, File chacheFiles[]) {
        this(documentSource, chacheFiles, new FileBasedTokenizedLabelRetriever[0], MAX_NUMBER_OF_WORKERS);
    }

    @Deprecated
    public WorkerBasedLabelRetrievingDocumentSupplierDecorator(DocumentSupplier supplier, boolean b) {
        this(supplier, ThreadSafeCachingLabelTokenizerDecorator.DEFAULT_FILES);
    }

    @Deprecated
    public WorkerBasedLabelRetrievingDocumentSupplierDecorator(DocumentSupplier supplier, boolean b, boolean c) {
        this(supplier, ThreadSafeCachingLabelTokenizerDecorator.DEFAULT_FILES);
    }

    @Override
    protected void editDocumentProperty(StringCountMapping mapping) {
        ObjectLongOpenHashMap<String> countedUris = mapping.get();
        countedTokens = new ObjectLongOpenHashMap<String>();
        for (int i = 0; i < countedUris.allocated.length; ++i) {
            if (countedUris.allocated[i]) {
                try {
                    activeWorkers.acquire();
                } catch (InterruptedException e) {
                    LOGGER.error("Exception while waiting for workers.", e);
                }
                // (new Thread(new Worker((String) ((Object[])
                // countedUris.keys)[i], countedUris.values[i],
                // localTokenizer, fileBasedLabelTokenizer,
                // clientLabelTokenizer, this))).start();
                overseer.startTask(new Worker((String) ((Object[]) countedUris.keys)[i], countedUris.values[i],
                        localTokenizer, localLabelTokenizers, clientLabelTokenizer, this));
            }
        }
        // Make sure that all workers have finished
        try {
            activeWorkers.acquire(MAX_NUMBER_OF_WORKERS);
        } catch (InterruptedException e) {
            LOGGER.error("Exception while waiting for workers.", e);
        }
        activeWorkers.release(MAX_NUMBER_OF_WORKERS);

        mapping.set(countedTokens);
    }

    public void storeCache() {
        clientLabelTokenizer.storeCache();
    }

    protected synchronized void workerFinished(List<String> tokens, long count) {
        for (String token : tokens) {
            countedTokens.putOrAdd(token, count, count);
        }
        activeWorkers.release();
    }

    private static TokenizedLabelRetriever[] createFileBasedTokenizers(File[] labelFiles) {
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
        return tempRetrievers.toArray(new TokenizedLabelRetriever[tempRetrievers.size()]);
    }

    protected static class Worker extends AbstractWaitingTask {
        private String uri;
        private long count;
        private LocalLabelTokenizer localTokenizer;
        private TokenizedLabelRetriever localLabelTokenizers[];
        private ThreadSafeCachingLabelTokenizerDecorator clientLabelTokenizer;
        private WorkerBasedLabelRetrievingDocumentSupplierDecorator observer;
        private RDFClientLabelRetriever rdfClient = new RDFClientLabelRetriever();

        public Worker(String uri, long count, LocalLabelTokenizer localTokenizer,
                TokenizedLabelRetriever localLabelTokenizers[],
                ThreadSafeCachingLabelTokenizerDecorator clientLabelTokenizer,
                WorkerBasedLabelRetrievingDocumentSupplierDecorator observer) {
            this.uri = uri;
            this.count = count;
            this.localTokenizer = localTokenizer;
            this.localLabelTokenizers = localLabelTokenizers;
            this.clientLabelTokenizer = clientLabelTokenizer;
            this.observer = observer;
        }

        @Override
        public void run() {
            // extract namespace
            String namespace = extractVocabulary(uri);

            // Get the tokens of the label
            List<String> tokens = null;
            for (int i = 0; (tokens == null) && (i < localLabelTokenizers.length); ++i) {
                tokens = localLabelTokenizers[i].getTokenizedLabel(uri, namespace);
            }
            // If the label couldn't be retrieved
            if (tokens == null) {
                this.startWaiting();
                tokens = clientLabelTokenizer.getTokenizedLabel(rdfClient, uri, namespace);
                this.stopWaiting();
            }
            // If the label couldn't be retrieved, create it based on the URI
            if ((tokens == null) || (tokens.size() == 0)) {
                tokens = localTokenizer.getTokenizedLabel(uri, namespace);
            }

            observer.workerFinished(tokens, count);
        }

        private String extractVocabulary(String uri) {
            String namespace = null;
            // check the vocabs of the set
            // for (String vocab : vocabs) {
            // if (uri.startsWith(vocab)) {
            // if ((namespace == null) || (namespace.length() < vocab.length()))
            // {
            // namespace = vocab;
            // }
            // }
            // }
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

        @Override
        public String getId() {
            return uri;
        }

        @Override
        public String getProgress() {
            return null;
        }
    }

    public static class ExceptionThrowingRetriever implements TokenizedLabelRetriever {

        @Override
        public List<String> getTokenizedLabel(String uri, String namespace) {
            throw new IllegalArgumentException();
        }

    }

    @Override
    public void close() {
        overseer.shutdown();
    }
}