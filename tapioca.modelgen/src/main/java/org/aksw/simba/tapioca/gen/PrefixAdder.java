package org.aksw.simba.tapioca.gen;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.shared.PrefixMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrefixAdder {

    private static final int MIN_COUNT_FOR_PREFIX = 3;

    private static final Logger LOGGER = LoggerFactory.getLogger(PrefixAdder.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            LOGGER.error("Not enough arguments:\nPrefixAdder <input-file> <output-file> <lang>");
        }
        LOGGER.info("Reading file...");
        Model model = ModelFactory.createDefaultModel();
        try (InputStream is = new BufferedInputStream(new FileInputStream(args[0]))) {
            model.read(is, "", args[2]);
        } catch (Exception e) {
            LOGGER.error("Exception while reading the input file.", e);
            return;
        }
        LOGGER.info("Searching for namespaces and adding prefixes...");
        addPrefixes(model);
        LOGGER.info("Writing file...");
        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(args[1]))) {
            model.write(os, args[2]);
        } catch (Exception e) {
            LOGGER.error("Exception while writing the output file.", e);
            return;
        }
    }

    public static void addPrefixes(Model model) {
        StmtIterator iterator = model.listStatements();
        Map<String, Long> namespaceCounts = StreamSupport
                .stream(Spliterators.spliterator(iterator, model.size(), Spliterator.DISTINCT | Spliterator.NONNULL),
                        true)
                .flatMap(s -> extractNamespaces(s).stream())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        PrefixMapping mapping = PrefixMapping.Factory.create();
        int prefixCount = 0;
        for (Entry<String, Long> e : namespaceCounts.entrySet()) {
            if ((!e.getKey().isEmpty()) && (e.getValue() >= MIN_COUNT_FOR_PREFIX)) {
                mapping.setNsPrefix("ns" + prefixCount, e.getKey());
                ++prefixCount;
            }
        }
        model.setNsPrefixes(mapping);
    }

    public static List<String> extractNamespaces(Statement s) {
        List<String> namespaces = new ArrayList<String>(3);
        Resource r = s.getSubject();
        if (r.isURIResource()) {
            namespaces.add(r.getNameSpace());
        }
        namespaces.add(s.getPredicate().getNameSpace());
        RDFNode o = s.getObject();
        if (o.isURIResource()) {
            namespaces.add(o.asResource().getNameSpace());
        } else if (o.isLiteral()) {
            RDFDatatype d = o.asLiteral().getDatatype();
            if (d != null) {
                namespaces.add(d.getURI().substring(0, Util.splitNamespaceXML(d.getURI())));
            }
        }
        return namespaces;
    }
}
