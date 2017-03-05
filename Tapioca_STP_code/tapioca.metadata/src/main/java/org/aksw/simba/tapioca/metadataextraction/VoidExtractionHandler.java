/**
 * This package counts classes and properties within a 
 * RDF data set.
 */
package org.aksw.simba.tapioca.metadataextraction;

import java.util.concurrent.ExecutorService;

import org.aksw.simba.tapioca.cores.data.VOID;
import org.aksw.simba.tapioca.cores.data.VoidInformation;
import org.aksw.simba.tapioca.cores.extraction.VoidExtractor;
import org.aksw.simba.tapioca.cores.extraction.VoidParsingExtractor;
import org.aksw.simba.tapioca.cores.helper.AbstractDumpExtractorApplier;

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
 * Execute the VoID extraction
 * 
 * @author Kai
 *
 */
public class VoidExtractionHandler extends AbstractDumpExtractorApplier {

	/**
	 * Logger for errors, warnings and other informations.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(VoidExtractionHandler.class);

	/**
	 * Constructor
	 */
	public VoidExtractionHandler() {
		super(null);
	}

	/**
	 * Constructor
	 * 
	 * @param executor
	 *            Executor
	 */
	public VoidExtractionHandler(ExecutorService executor) {
		super(executor);
	}

	/**
	 * Insert VoID extractor information into VoID parsing extractor
	 * 
	 * @param extractor
	 *            VoID extractor
	 * @param vpExtractor
	 *            VoID parsing extractor
	 */
	protected void addParsedVoidToCounts(VoidExtractor extractor, VoidParsingExtractor vpExtractor) {
		// get void information
		ObjectObjectOpenHashMap<String, VoidInformation> voidInfo = vpExtractor.getVoidInformation();
		// get counted classes
		ObjectIntOpenHashMap<String> countedClasses = extractor.getCountedClasses();
		// get counted properties
		ObjectIntOpenHashMap<String> countedProperties = extractor.getCountedProperties();
		// adding the information
		for (int i = 0; i < voidInfo.allocated.length; ++i) {
			if (voidInfo.allocated[i]) {
				((VoidInformation) (((Object[]) voidInfo.values)[i])).addToCount(countedClasses, countedProperties);
			}
		}
	}

	/**
	 * Add counted URIs
	 * 
	 * @param countedUris
	 *            Counted URIs
	 * @param voidModel
	 *            VoID model
	 * @param datasetResource
	 *            Data set resource
	 * @param partitionProperty
	 *            partition property
	 * @param uriProperty
	 *            URI property
	 * @param countProperty
	 *            Count property
	 * @return Sum of elements
	 */
	protected long addCountedUris(ObjectIntOpenHashMap<String> countedUris, Model voidModel, Resource datasetResource,
			Property partitionProperty, Property uriProperty, Property countProperty) {
		// initialize
		long sum = 0;
		Resource blank;

		// loop counted uris
		for (int i = 0; i < countedUris.allocated.length; ++i) {
			// if not empty, then...
			if (countedUris.allocated[i]) {
				// insert blank node
				blank = voidModel.createResource();
				// link it with partition property to data set resoruce
				voidModel.add(datasetResource, partitionProperty, blank);
				// add uri
				voidModel.add(blank, uriProperty, voidModel.createResource((String) ((Object[]) countedUris.keys)[i]));
				// add counts
				voidModel.addLiteral(blank, countProperty, countedUris.values[i]);
				// increment sum
				sum += countedUris.values[i];
			}
		}

		// return result
		return sum;
	}

	/**
	 * Generate the VoID model
	 * 
	 * @param datsetUri
	 *            URI of the data set
	 * @param extractor
	 *            VoID extractor
	 * @return VoID model
	 */
	protected Model generateVoidModel(String datsetUri, VoidExtractor extractor) {
		// create new model
		Model voidModel = ModelFactory.createDefaultModel();
		// insert data set as resource
		Resource datasetResource = voidModel.createResource(datsetUri);
		// state that it is a data set
		voidModel.add(datasetResource, RDF.type, VOID.Dataset);

		// add counted classes
		long entities = addCountedUris(extractor.getCountedClasses(), voidModel, datasetResource, VOID.classPartition,
				VOID.clazz, VOID.entities);
		// add number of classes
		voidModel.addLiteral(datasetResource, VOID.classes, extractor.getCountedClasses().assigned);
		// add number of entities
		voidModel.addLiteral(datasetResource, VOID.entities, entities);

		// add counted properties
		long triples = addCountedUris(extractor.getCountedProperties(), voidModel, datasetResource,
				VOID.propertyPartition, VOID.property, VOID.triples);
		// add number of classes
		voidModel.addLiteral(datasetResource, VOID.properties, extractor.getCountedProperties().assigned);
		// add number of triples
		voidModel.addLiteral(datasetResource, VOID.triples, triples);

		// handle errors
		if ((entities == 0) && (triples == 0)) {
			LOGGER.error("Got an empty VOID model without an entity and a triple. Returning null.");
			return null;
		}

		// return result
		return voidModel;
	}

	/**
	 * Extracts the VoID information of an RDF data set comprising the given
	 * dump files.
	 * 
	 * @param datasetUri
	 *            URI of the data set
	 * @param inFile
	 *            The input file
	 * @return A VoID model with the VoID information
	 */
	public Model extractVoidInfo(String datasetUri, String inFile) {
		// create void extractor
		VoidExtractor extractor = new VoidExtractor();
		// create void parsing extractor
		VoidParsingExtractor vpExtractor = new VoidParsingExtractor();

		// extract from input file
		if (!extractFromDump(inFile, extractor, vpExtractor)) {
			LOGGER.error("Couldn't extract information from dump \"" + inFile + "\". Returning null.");
			return null;
		}
		// insert void extractor information into void parsing extractor
		addParsedVoidToCounts(extractor, vpExtractor);

		// return result
		return generateVoidModel(datasetUri, extractor);
	}
}
