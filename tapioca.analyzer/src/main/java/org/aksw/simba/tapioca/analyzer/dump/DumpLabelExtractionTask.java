package org.aksw.simba.tapioca.analyzer.dump;

import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.aksw.simba.tapioca.analyzer.label.LabelExtractionUtils;
import org.aksw.simba.topicmodeling.commons.io.StorageHelper;
import org.aksw.simba.topicmodeling.concurrent.tasks.Task;

public class DumpLabelExtractionTask implements Task {

	protected String dumps[];
    protected File voidFile;
    protected File outputFile;
    protected DumpFileLabelExtractor extractor;

    public DumpLabelExtractionTask(String dumps[], File voidFile, File outputFile) {
        this(dumps, voidFile, outputFile, null);
    }

    public DumpLabelExtractionTask(String dumps[], File voidFile, File outputFile, ExecutorService executor) {
        this.dumps = dumps;
        this.voidFile = voidFile;
        this.outputFile = outputFile;
        if (executor != null) {
            extractor = new DumpFileLabelExtractor(executor);
        } else {
            extractor = new DumpFileLabelExtractor();
        }
    }

    @Override
    public void run() {
        // read URIs from void file
        Set<String> uris = LabelExtractionUtils.readUris(voidFile);
        if (uris == null) {
            return;
        }
        String labels[][] = extractor.extractLabels(uris, dumps);
        if (labels != null) {
            StorageHelper.storeToFileSavely(labels, outputFile.getAbsolutePath());
            labels = null;
        }
    }

    @Override
    public String getId() {
        return "LabelExtraction" + Arrays.toString(dumps);
    }

    @Override
    public String getProgress() {
        return "";
    }
}
