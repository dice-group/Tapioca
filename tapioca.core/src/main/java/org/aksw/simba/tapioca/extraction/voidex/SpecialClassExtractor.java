package org.aksw.simba.tapioca.extraction.voidex;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aksw.simba.tapioca.extraction.AbstractExtractor;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDF;

public class SpecialClassExtractor extends AbstractExtractor {

	private static final Logger LOGGER = LoggerFactory.getLogger(SpecialClassExtractor.class);

	private static final String SPECIAL_CLASSES_RESOURCE_NAME = "special_classes.txt";
	private static final Set<Node> SPECIAL_CLASSES = loadList(SPECIAL_CLASSES_RESOURCE_NAME);

	private ObjectIntOpenHashMap<String> countedSpecialClasses;

	public SpecialClassExtractor() {
		this.countedSpecialClasses = new ObjectIntOpenHashMap<String>();
	}

	public SpecialClassExtractor(ObjectIntOpenHashMap<String> countedSpecialClasses) {
		this.countedSpecialClasses = countedSpecialClasses;
	}

	public void handleTriple(Triple triple) {
		Node subject = triple.getSubject();
		if (triple.getPredicate().equals(RDF.type.asNode()) && !(subject.isBlank())) {
			if (SPECIAL_CLASSES.contains(triple.getObject())) {
				countedSpecialClasses.putOrAdd(subject.getURI(), 0, 0);
			}
		}
	}

	public ObjectIntOpenHashMap<String> getCountedSpecialClasses() {
		return countedSpecialClasses;
	}

	public void setCountedSpecialClasses(ObjectIntOpenHashMap<String> countedSpecialClasses) {
		this.countedSpecialClasses = countedSpecialClasses;
	}

	protected static Set<Node> loadList(String listName) {
		InputStream is = SpecialClassExtractor.class.getClassLoader().getResourceAsStream(listName);
		if (is == null) {
			LOGGER.error("Couldn't load list " + listName + " from resources. Returning empty list.");
			return new HashSet<Node>();
		}
		List<String> lines;
		try {
			lines = IOUtils.readLines(is);
		} catch (IOException e) {
			LOGGER.error("Couldn't load list from resources. Returning empty list.", e);
			return new HashSet<Node>();
		}
		Set<Node> resourceList = new HashSet<Node>((int) 2 * lines.size());
		for (String line : lines) {
			resourceList.add(ResourceFactory.createResource(line.trim()).asNode());
		}
		return resourceList;
	}
}
