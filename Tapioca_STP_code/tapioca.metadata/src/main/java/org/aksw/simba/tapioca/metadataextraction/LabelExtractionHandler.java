/**
 * This package extracts labels from a RDF data set.
 */
package org.aksw.simba.tapioca.metadataextraction;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.aksw.simba.tapioca.cores.data.VOID;
import org.aksw.simba.tapioca.cores.extraction.LabelExtractor;
import org.aksw.simba.tapioca.cores.helper.AbstractDumpExtractorApplier;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * The handler for the label extraction. *
 * 
 * @author Kai
 * 
 */
public class LabelExtractionHandler extends AbstractDumpExtractorApplier {

	// -------------------------------------------------------------------------
	// ------------- Variables -------------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * VoID model, which contains labels.
	 */
	private Model voidModel;

	/**
	 * URIs, which are used to extract labels.
	 */
	private Set<String> uris;

	/**
	 * Mapping of URIs to their labels. The map contains the URIs as keys and a
	 * set as values. The set contains all labels for a specific URI.
	 */
	private Map<String, Set<String>> labels;

	// -------------------------------------------------------------------------
	// ------------- Methods ---------------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Constructor. Set the super class' executor service to null.
	 * 
	 * @param voidModel
	 *            VoID Model
	 */
	public LabelExtractionHandler() {
		super(null);
	}

	/**
	 * Constructor. Set the super class' executor service to the specific
	 * executor.
	 * 
	 * @param executor
	 *            Executor
	 */
	public LabelExtractionHandler(ExecutorService executor) {
		super(executor);
	}

	/**
	 * Read required URIs and save them in a set.
	 */
	protected void readRequiredUris() {
		// create new set of strings
		Set<String> uris = new HashSet<String>();

		// get all resources with property "VOID.clazz"
		ResIterator iterClasses = voidModel.listResourcesWithProperty(VOID.clazz);
		while (iterClasses.hasNext()) {
			String classUri = iterClasses.nextResource().getProperty(VOID.clazz).getObject().toString();
			uris.add(classUri);
		}

		// get all resources with property "VOID.property"
		ResIterator iterPorperties = voidModel.listResourcesWithProperty(VOID.property);
		while (iterPorperties.hasNext()) {
			String propertyUri = iterPorperties.nextResource().getProperty(VOID.property).getObject().toString();
			uris.add(propertyUri);
		}

		// save the URIs
		this.uris = uris;
	}

	/**
	 * Extract labels from input file.
	 * 
	 * @param inFile
	 *            Input file
	 */
	protected void runExtraction(String inFile) {
		// create label extractor
		LabelExtractor extractor = new LabelExtractor(uris);
		// run extraction
		extractFromDump(inFile, extractor);
		// get labels
		Map<String, Set<String>> labels = extractor.getLabels();
		// save the labels
		this.labels = labels;
	}

	/**
	 * Add extracted labels to VoID Model
	 */
	protected void addExtractedLabels() {
		// loop all entries in hash map
		for (Map.Entry<String, Set<String>> entry : labels.entrySet()) {
			String uri = entry.getKey();
			Set<String> labelsOfUri = entry.getValue();
			// loop all labels of uri
			for (String label : labelsOfUri) {
				// add label to model
				voidModel.getResource(uri).addProperty(RDFS.label, label);
			}
		}
	}

	/**
	 * Extract labels from input file and return the VoID model.
	 * 
	 * @param inFile
	 *            Input file
	 * @return VoID model with extracted labels
	 */
	public Model extractLabels(String inFile) {
		// read URIs
		readRequiredUris();
		// extract labels
		runExtraction(inFile);
		// add labels to model
		addExtractedLabels();
		// return model
		return voidModel;
	}

	/**
	 * Set VoID Model
	 * 
	 * @param voidModel
	 *            VoID Model
	 */
	public void setVoidModel(Model voidModel) {
		this.voidModel = voidModel;
	}

	/**
	 * Return the URIs. The URIs are saved in set of strings.
	 * 
	 * @return URIs
	 */
	protected Set<String> getUris() {
		return uris;
	}

	/**
	 * Return the VoID model.
	 * 
	 * @return VoID Model
	 */
	protected Model getVoidModel() {
		return voidModel;
	}

	/**
	 * Return the labels. The labels are saved in a map with the URIs as keys
	 * and a a set as values. The set contains all labels for the specific URI.
	 * 
	 * @return Labels
	 */
	protected Map<String, Set<String>> getLabels() {
		return labels;
	}

}
