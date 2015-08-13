package org.aksw.simba.tapioca.analyzer.label;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.aksw.simba.tapioca.data.DatasetClassInfo;
import org.aksw.simba.tapioca.data.DatasetPropertyInfo;
import org.aksw.simba.tapioca.preprocessing.JenaBasedVoidParsingSupplierDecorator;
import org.aksw.simba.tapioca.preprocessing.labelretrieving.LabelTokenizerHelper;
import org.aksw.simba.topicmodeling.io.SimpleDocSupplierFromFile;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.DocumentTextCreatingSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;

public class LabelExtractionUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(LabelExtractionUtils.class);

	public static Set<String> readUris(File voidFile) {
		SimpleDocSupplierFromFile reader = new SimpleDocSupplierFromFile();
		reader.createRawDocumentAdHoc(voidFile);
		DocumentSupplier supplier = new DocumentTextCreatingSupplierDecorator(reader);
		supplier = new JenaBasedVoidParsingSupplierDecorator(supplier);

		Document document = supplier.getNextDocument();
		DatasetClassInfo classInfo = document.getProperty(DatasetClassInfo.class);
		Set<String> uris = new HashSet<String>();
		ObjectLongOpenHashMap<String> countedURIs;
		if (classInfo != null) {
			countedURIs = classInfo.get();
			for (int i = 0; i < countedURIs.allocated.length; ++i) {
				if (countedURIs.allocated[i]) {
					uris.add((String) ((Object[]) countedURIs.keys)[i]);
				}
			}
		} else {
			LOGGER.error("Document doesn't contain class information. Returning null.");
			return null;
		}

		DatasetPropertyInfo propInfo = document.getProperty(DatasetPropertyInfo.class);
		if (propInfo != null) {
			countedURIs = propInfo.get();
			for (int i = 0; i < countedURIs.allocated.length; ++i) {
				if (countedURIs.allocated[i]) {
					uris.add((String) ((Object[]) countedURIs.keys)[i]);
				}
			}
		} else {
			LOGGER.error("Document doesn't contain property information. Returning null.");
			return null;
		}
		return uris;
	}

	public static String[][] generateArray(Map<String, Set<String>> labels) {
		String uriToLabel[][] = new String[labels.size()][];
		int pos = 0;
		String tokenizedLabel[];
		for (Entry<String, Set<String>> labelsOfUri : labels.entrySet()) {
			tokenizedLabel = tokenize(labelsOfUri.getValue());
			if (tokenizedLabel != null) {
				uriToLabel[pos] = new String[tokenizedLabel.length + 1];
				System.arraycopy(tokenizedLabel, 0, uriToLabel[pos], 1, tokenizedLabel.length);
			} else {
				uriToLabel[pos] = new String[1];
			}
			uriToLabel[pos][0] = labelsOfUri.getKey();
			++pos;
		}
		return uriToLabel;
	}

	public static String[] tokenize(Set<String> names) {
		HashSet<String> uniqueLabels = new HashSet<String>();
		for (String label : names) {
			uniqueLabels.addAll(LabelTokenizerHelper.getSeparatedText(label));
		}
		return uniqueLabels.toArray(new String[uniqueLabels.size()]);
	}
}
