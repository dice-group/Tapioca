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
package org.aksw.simba.tapioca.analyzer.hdt;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aksw.simba.tapioca.analyzer.label.LabelExtractionUtils;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.RDFClientLabelRetriever;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the analysis of given RDF dumps.
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class HdtLabelExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdtLabelExtractor.class);

    public HdtLabelExtractor() {
    }

    public String[][] extractLabels(File hdtDumpFile, Model voidModel) {
        return extractLabels(hdtDumpFile, LabelExtractionUtils.readUris(voidModel));
    }

    public String[][] extractLabels(File hdtDumpFile, Set<String> uris) {
        HDT hdt = null;
        try {
            hdt = HDTManager.loadIndexedHDT(hdtDumpFile.getAbsolutePath(), null);
            Map<String, Set<String>> labels = new HashMap<>();
            IteratorTripleString iterator;
            TripleString triple;
            String subject;
            Set<String> labelsOfUri;
            for (int i = 0; i < RDFClientLabelRetriever.NAMING_PROPERTIES.length; ++i) {
                iterator = hdt.search(null, RDFClientLabelRetriever.NAMING_PROPERTIES[i], null);
                while (iterator.hasNext()) {
                    triple = iterator.next();
                    subject = triple.getSubject().toString();
                    if (labels.containsKey(subject)) {
                        labelsOfUri = labels.get(subject);
                    } else {
                        labelsOfUri = new HashSet<String>();
                        labels.put(subject, labelsOfUri);
                    }
                    labelsOfUri.add(triple.getObject().toString());
                }
            }
            return LabelExtractionUtils.generateArray(labels);
        } catch (Exception e) {
            LOGGER.error("Couldn't read dump file \"" + hdtDumpFile + "\". Ignoring this dump.", e);
            return null;
        } finally {
            IOUtils.closeQuietly(hdt);
        }
    }
}
