/**
 * tapioca.analyzer - ${project.description}
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

import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.aksw.simba.tapioca.analyzer.label.LabelExtractionUtils;
import org.aksw.simba.topicmodeling.commons.io.StorageHelper;
import org.aksw.simba.topicmodeling.concurrent.tasks.Task;

/**
 * 
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
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
