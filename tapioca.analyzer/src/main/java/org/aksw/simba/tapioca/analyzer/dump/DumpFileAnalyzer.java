package org.aksw.simba.tapioca.analyzer.dump;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.zip.GZIPInputStream;

import org.aksw.simba.tapioca.data.vocabularies.EVOID;
import org.aksw.simba.tapioca.data.vocabularies.VOID;
import org.aksw.simba.tapioca.extraction.Extractor;
import org.aksw.simba.tapioca.extraction.RDF2ExtractionStreamer;
import org.aksw.simba.tapioca.extraction.voidex.SpecialClassExtractor;
import org.aksw.simba.tapioca.extraction.voidex.VoidExtractor;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.RDFLanguages;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

public class DumpFileAnalyzer {

    protected ExecutorService executor = null;

    public DumpFileAnalyzer() {
    }

    public DumpFileAnalyzer(ExecutorService executor) {
        this.executor = executor;
    }

    public Model extractVoidInfo(String datsetUri, String... dumps) {
        VoidExtractor extractor = new VoidExtractor();
        SpecialClassExtractor sExtractor = new SpecialClassExtractor();
        for (int i = 0; i < dumps.length; ++i) {
            analyzeDump(dumps[i], extractor, sExtractor);
        }
        return generateVoidModel(datsetUri, extractor, sExtractor);
    }

    protected void analyzeDump(String dump, Extractor... extractors) {
        RDF2ExtractionStreamer streamer;
        if (executor != null) {
            streamer = new RDF2ExtractionStreamer(executor);
        } else {
            streamer = new RDF2ExtractionStreamer();
        }
        InputStream fin = null;
        try {
            fin = new FileInputStream(dump);
            if (dump.endsWith(".gz")) {
                fin = new GZIPInputStream(fin);
                dump = dump.substring(0, dump.length() - 3);
            }
            streamer.runExtraction(fin, "", RDFLanguages.resourceNameToLang(dump), extractors);
        } catch (Exception e) {

        } finally {
            IOUtils.closeQuietly(fin);
        }
    }

    protected Model generateVoidModel(String datsetUri, VoidExtractor extractor, SpecialClassExtractor sExtractor) {
        Model voidModel = ModelFactory.createDefaultModel();
        Resource datasetResource = voidModel.createResource(datsetUri);

        voidModel.add(datasetResource, RDF.type, VOID.Dataset);

        long sum;
        sum = addCountedUris(extractor.getCountedClasses(), voidModel, datasetResource, VOID.classPartition, VOID.clazz,
                VOID.entities);
        sum += addCountedUris(sExtractor.getCountedSpecialClasses(), voidModel, datasetResource, EVOID.classPartition,
                EVOID.specialClass, EVOID.entities);
        voidModel.addLiteral(datasetResource, VOID.classes,
                extractor.getCountedClasses().assigned + sExtractor.getCountedSpecialClasses().assigned);
        voidModel.addLiteral(datasetResource, VOID.entities, sum);

        sum = addCountedUris(extractor.getCountedProperties(), voidModel, datasetResource, VOID.propertyPartition,
                VOID.property, VOID.triples);
        voidModel.addLiteral(datasetResource, VOID.properties, extractor.getCountedProperties().assigned);
        voidModel.addLiteral(datasetResource, VOID.triples, sum);

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
}
