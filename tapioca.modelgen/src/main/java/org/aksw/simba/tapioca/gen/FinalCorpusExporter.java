/**
 * tapioca.modelgen - ${project.description}
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
 * This file is part of tapioca.modelgen.
 *
 * tapioca.modelgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.modelgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.modelgen.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.gen;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.dice_research.topicmodeling.io.gzip.GZipCorpusObjectReader;
import org.dice_research.topicmodeling.utils.corpus.Corpus;
import org.dice_research.topicmodeling.utils.doc.Document;
import org.dice_research.topicmodeling.utils.doc.DocumentURI;
import org.apache.commons.io.IOUtils;
import org.apache.jena.n3.turtle.TurtleReader;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.RDFReader;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinalCorpusExporter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(FinalCorpusExporter.class);

    private static final String LOD_STATS_DOC_BASE_URI = "http://lodstats.aksw.org/rdfdocs/";

    public static void main(String[] args) {
        FinalCorpusExporter exporter = new FinalCorpusExporter();
        exporter.run(TMBasedIndexGenerator.OUTPUT_FOLDER + File.separator + TMBasedIndexGenerator.FINAL_CORPUS_FILE,
                "/Daten/tapioca/export.nt", TMBasedIndexGenerator.OUTPUT_FOLDER + File.separator
                        + TMBasedIndexGenerator.MODEL_META_DATA_FILE);
    }

    public void run(String corpusFile, String outputFile, String metaDataModelFile) {
        Corpus corpus = readCorpus(corpusFile);
        Model model = readModel(metaDataModelFile);
        Model exportModel = ModelFactory.createDefaultModel();

        DocumentURI uri;
        Resource datasetResource;
        for (Document document : corpus) {
            uri = document.getProperty(DocumentURI.class);
            if (uri != null) {
                datasetResource = new ResourceImpl(uri.get());
                if (model.containsResource(datasetResource)) {
                    exportModel.add(model.listStatements(datasetResource, null, (RDFNode) null));
                }
            }
        }

        FileOutputStream fout = null;
        try {
            fout = new FileOutputStream(outputFile);
            RDFDataMgr.write(fout, exportModel, RDFFormat.NTRIPLES);
        } catch (Exception e) {
            LOGGER.error("Error while writing result.", e);
        } finally {
            IOUtils.closeQuietly(fout);
        }
    }

    protected Corpus readCorpus(String corpusFile) {
        GZipCorpusObjectReader reader = new GZipCorpusObjectReader(new File(corpusFile));
        return reader.getCorpus();
    }

    protected Model readModel(String metaDataModelFile) {
        RDFReader reader = new TurtleReader();
        Model model = ModelFactory.createDefaultModel();
        FileInputStream fin = null;
        try {
            fin = new FileInputStream(metaDataModelFile);
            reader.read(model, fin, LOD_STATS_DOC_BASE_URI);
        } catch (FileNotFoundException e) {
            LOGGER.error("Couldn't read model with additional meta data from file. Ignoring this file.", e);
            return null;
        } finally {
            IOUtils.closeQuietly(fin);
        }
        return model;
    }
}
