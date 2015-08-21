package org.aksw.simba.tapioca.analyzer.dump;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.aksw.simba.tapioca.analyzer.label.LabelExtractionUtils;
import org.aksw.simba.tapioca.analyzer.label.LabelExtractor;

public class DumpFileLabelExtractor extends AbstractDumpExtractorApplier {

    public DumpFileLabelExtractor() {
        super(null);
    }

    public DumpFileLabelExtractor(ExecutorService executor) {
        super(executor);
    }

    public String[][] extractLabels(Set<String> uris, String... dumps) {
        LabelExtractor extractor = new LabelExtractor(uris);
        for (int i = 0; i < dumps.length; ++i) {
            extractFromDump(dumps[i], extractor);
        }
        return LabelExtractionUtils.generateArray(extractor.getLabels());
    }

}
