package org.aksw.simba.tapioca.preprocessing.labelretrieving;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.aksw.simba.tapioca.data.StringCountMapping;
import org.aksw.simba.topicmodeling.concurrent.overseers.pool.DefeatableOverseer;
import org.aksw.simba.topicmodeling.concurrent.overseers.pool.ExecutorBasedOverseer;
import org.aksw.simba.topicmodeling.concurrent.reporter.LogReporter;
import org.aksw.simba.topicmodeling.concurrent.reporter.Reporter;
import org.aksw.simba.topicmodeling.concurrent.tasks.Task;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyEditingDocumentSupplierDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;

public class WorkerBasedLabelRetrievingDocumentSupplierDecorator extends
        AbstractPropertyEditingDocumentSupplierDecorator<StringCountMapping> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(WorkerBasedLabelRetrievingDocumentSupplierDecorator.class);

    // private static final int MAX_NUMBER_OF_WORKERS = 20;
    private static final int MAX_NUMBER_OF_WORKERS = 1;

    private LocalLabelTokenizer localTokenizer = new LocalLabelTokenizer();
    private FileBasedTokenizedLabelRetriever fileBasedLabelTokenizers[];
    private ThreadSafeCachingLabelTokenizerDecorator clientLabelTokenizer;
    private Semaphore activeWorkers = new Semaphore(MAX_NUMBER_OF_WORKERS);
    private ObjectLongOpenHashMap<String> countedTokens;
    private DefeatableOverseer overseer = new ExecutorBasedOverseer(MAX_NUMBER_OF_WORKERS);
    @SuppressWarnings("unused")
    private Reporter reporter = new LogReporter(overseer);

    public WorkerBasedLabelRetrievingDocumentSupplierDecorator(DocumentSupplier documentSource, File chacheFiles[]) {
        super(documentSource, StringCountMapping.class);
        // fileBasedLabelTokenizer = FileBasedTokenizedLabelRetriever.create();
        clientLabelTokenizer = ThreadSafeCachingLabelTokenizerDecorator.create(new RDFClientLabelRetriever(),
                chacheFiles);
        fileBasedLabelTokenizers = new FileBasedTokenizedLabelRetriever[0];
    }

    public WorkerBasedLabelRetrievingDocumentSupplierDecorator(DocumentSupplier documentSource, File chacheFiles[],
            File... labelFiles) {
        super(documentSource, StringCountMapping.class);
        List<FileBasedTokenizedLabelRetriever> tempRetrievers = new ArrayList<FileBasedTokenizedLabelRetriever>();
        FileBasedTokenizedLabelRetriever tempRetriever;
        for (int i = 0; i < labelFiles.length; ++i) {
            tempRetriever = FileBasedTokenizedLabelRetriever.create(labelFiles[i].getAbsolutePath());
            if (tempRetriever == null) {
                LOGGER.warn("Couldn't load labels from {}.", labelFiles[i]);
            } else {
                tempRetrievers.add(tempRetriever);
            }
        }
        fileBasedLabelTokenizers = tempRetrievers.toArray(new FileBasedTokenizedLabelRetriever[tempRetrievers.size()]);
        clientLabelTokenizer = ThreadSafeCachingLabelTokenizerDecorator.create(new RDFClientLabelRetriever(),
                chacheFiles);
    }

    // @Override
    // protected Document prepareDocument(Document document) {
    // StringCountMapping mapping =
    // document.getProperty(StringCountMapping.class);
    // if (mapping == null) {
    // LOGGER.error("Got a document without the needed StringCountMapping property. Ignoring this document.");
    // } else {
    // replaceURIsWithLabelTokens(mapping);
    // }
    // return document;
    // }

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
                        localTokenizer, fileBasedLabelTokenizers, clientLabelTokenizer, this));
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

    protected static class Worker implements Task {
        private String uri;
        private long count;
        private LocalLabelTokenizer localTokenizer;
        private FileBasedTokenizedLabelRetriever fileBasedLabelTokenizers[];
        private ThreadSafeCachingLabelTokenizerDecorator clientLabelTokenizer;
        private WorkerBasedLabelRetrievingDocumentSupplierDecorator observer;

        public Worker(String uri, long count, LocalLabelTokenizer localTokenizer,
                FileBasedTokenizedLabelRetriever fileBasedLabelTokenizers[],
                ThreadSafeCachingLabelTokenizerDecorator clientLabelTokenizer,
                WorkerBasedLabelRetrievingDocumentSupplierDecorator observer) {
            this.uri = uri;
            this.count = count;
            this.localTokenizer = localTokenizer;
            this.fileBasedLabelTokenizers = fileBasedLabelTokenizers;
            this.clientLabelTokenizer = clientLabelTokenizer;
            this.observer = observer;
        }

        @Override
        public void run() {
            // extract namespace
            String namespace = extractVocabulary(uri);

            // Get the tokens of the label
            List<String> tokens = null;
            for (int i = 0; (tokens == null) && (i < fileBasedLabelTokenizers.length); ++i) {
                tokens = fileBasedLabelTokenizers[i].getTokenizedLabel(uri, namespace);
            }
            // If the label couldn't be retrieved
            if (tokens == null) {
                tokens = clientLabelTokenizer.getTokenizedLabel(uri, namespace);
            }
            // If the label couldn't be retrieved, create it based on the URI
            if (tokens == null) {
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
                    namespace = uri;
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

    public void close() {
        overseer.shutdown();
    }
}
