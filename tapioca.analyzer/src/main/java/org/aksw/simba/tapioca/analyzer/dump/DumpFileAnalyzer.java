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

import java.util.concurrent.ExecutorService;

import org.aksw.simba.tapioca.data.vocabularies.EVOID;
import org.aksw.simba.tapioca.data.vocabularies.VOID;
import org.aksw.simba.tapioca.extraction.voidex.SpecialClassExtractor;
import org.aksw.simba.tapioca.extraction.voidex.VoidExtractor;
import org.aksw.simba.tapioca.extraction.voidex.VoidInformation;
import org.aksw.simba.tapioca.extraction.voidex.VoidParsingExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

/**
 * This class implements the analysis of given RDF dumps.
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class DumpFileAnalyzer extends AbstractDumpExtractorApplier {

    private static final Logger LOGGER = LoggerFactory.getLogger(DumpFileAnalyzer.class);

    public DumpFileAnalyzer() {
        super(null);
    }

    public DumpFileAnalyzer(ExecutorService executor) {
        super(executor);
    }

    /**
     * Extracts the VoID information of an RDF dataset comprising the given dump files.
     * 
     * @param datsetUri the URI of the dataset
     * @param dumps the dump files of the dataset
     * @return a Model containing the VoID information
     */
    public Model extractVoidInfo(String datsetUri, String... dumps) {
        VoidExtractor extractor = new VoidExtractor();
        SpecialClassExtractor sExtractor = new SpecialClassExtractor();
        VoidParsingExtractor vpExtractor = new VoidParsingExtractor();
        for (int i = 0; i < dumps.length; ++i) {
            if (!extractFromDump(dumps[i], extractor, sExtractor, vpExtractor)) {
                LOGGER.error("Couldn't extract information from dump \"" + dumps[i] + "\". Returning null.");
                return null;
            }
        }
        addParsedVoidToCounts(extractor, vpExtractor);
        return generateVoidModel(datsetUri, extractor, sExtractor);
    }

    protected Model generateVoidModel(String datsetUri, VoidExtractor extractor, SpecialClassExtractor sExtractor) {
        Model voidModel = ModelFactory.createDefaultModel();
        Resource datasetResource = voidModel.createResource(datsetUri);

        voidModel.add(datasetResource, RDF.type, VOID.Dataset);

        long entities;
        entities = addCountedUris(extractor.getCountedClasses(), voidModel, datasetResource, VOID.classPartition,
                VOID.clazz, VOID.entities);
        addCountedUris(sExtractor.getCountedSpecialClasses(), voidModel, datasetResource, EVOID.classPartition,
                EVOID.specialClass, EVOID.entities);
        voidModel.addLiteral(datasetResource, VOID.classes,
                extractor.getCountedClasses().assigned + sExtractor.getCountedSpecialClasses().assigned);
        voidModel.addLiteral(datasetResource, VOID.entities, entities);

        long triples = addCountedUris(extractor.getCountedProperties(), voidModel, datasetResource,
                VOID.propertyPartition, VOID.property, VOID.triples);
        voidModel.addLiteral(datasetResource, VOID.properties, extractor.getCountedProperties().assigned);
        voidModel.addLiteral(datasetResource, VOID.triples, triples);
        if ((entities == 0) && (triples == 0)) {
            LOGGER.error("Got an empty VOID model without an entity and a triple. Returning null.");
            return null;
        }
        return voidModel;
    }

    protected long addCountedUris(ObjectIntOpenHashMap<String> countedUris, Model voidModel, Resource datasetResource,
            Property partitionProperty, Property uriProperty, Property countProperty) {
        long sum = 0;
        Resource blank;
        for (int i = 0; i < countedUris.allocated.length; ++i) {
            if (countedUris.allocated[i]) {
                blank = voidModel.createResource();
                voidModel.add(datasetResource, partitionProperty, blank);
                voidModel.add(blank, uriProperty, voidModel.createResource((String) ((Object[]) countedUris.keys)[i]));
                voidModel.add(blank, uriProperty, voidModel.createResource((String) ((Object[]) countedUris.keys)[i]));
                voidModel.addLiteral(blank, countProperty, countedUris.values[i]);
                sum += countedUris.values[i];
            }
        }
        return sum;
    }

    protected void addParsedVoidToCounts(VoidExtractor extractor, VoidParsingExtractor vpExtractor) {
        ObjectObjectOpenHashMap<String, VoidInformation> voidInfo = vpExtractor.getVoidInformation();
        ObjectIntOpenHashMap<String> countedClasses = extractor.getCountedClasses();
        ObjectIntOpenHashMap<String> countedProperties = extractor.getCountedProperties();
        for (int i = 0; i < voidInfo.allocated.length; ++i) {
            if (voidInfo.allocated[i]) {
                ((VoidInformation) (((Object[]) voidInfo.values)[i])).addToCount(countedClasses, countedProperties);
            }
        }
    }
}
