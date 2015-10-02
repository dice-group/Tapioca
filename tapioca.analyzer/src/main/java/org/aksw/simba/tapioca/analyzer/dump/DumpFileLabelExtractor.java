/**
 * This file is part of tapioca.analyzer.
 *
 * tapioca.analyzer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.analyzer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.analyzer.  If not, see <http://www.gnu.org/licenses/>.
 */
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
