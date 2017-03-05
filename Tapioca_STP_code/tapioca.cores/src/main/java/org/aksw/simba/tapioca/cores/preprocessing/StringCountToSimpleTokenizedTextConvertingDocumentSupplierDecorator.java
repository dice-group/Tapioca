/**
 * This file is part of tapioca.cores.
 *
 * tapioca.cores is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.cores is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.cores.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.cores.preprocessing;

import java.util.ArrayList;
import java.util.List;

import org.aksw.simba.tapioca.cores.data.SimpleTokenizedText;
import org.aksw.simba.tapioca.cores.data.StringCountMapping;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.DocumentSupplier;
import org.aksw.simba.topicmodeling.preprocessing.docsupplier.decorator.AbstractPropertyAppendingDocumentSupplierDecorator;
import org.aksw.simba.topicmodeling.utils.doc.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;

/**
 * @author Michael Roeder, Marleen W.
 */
public class StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator
		extends AbstractPropertyAppendingDocumentSupplierDecorator<SimpleTokenizedText> {

	public static enum WordOccurence {
		UNIQUE, LOG
	}

	private static final Logger LOGGER = LoggerFactory
			.getLogger(StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.class);

	private WordOccurence occurence;

	public StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator(DocumentSupplier documentSource,
			WordOccurence occurence) {
		super(documentSource);
		this.occurence = occurence;
	}

	@Override
	protected SimpleTokenizedText createPropertyForDocument(Document document) {
		StringCountMapping mapping = document.getProperty(StringCountMapping.class);
		if (mapping == null) {
			LOGGER.error("Got a document without the needed StringCountMapping property. Ignoring this document.");
		} else {
			return createSimpleTokenizedText(mapping);
		}
		return null;
	}

	private SimpleTokenizedText createSimpleTokenizedText(StringCountMapping mapping) {
		ObjectLongOpenHashMap<String> counts = mapping.get();
		String token;
		long count;
		List<String> tokens = new ArrayList<String>(counts.size());
		for (int i = 0; i < counts.allocated.length; ++i) {
			if (counts.allocated[i]) {
				token = (String) ((Object[]) counts.keys)[i];
				count = counts.values[i];

				switch (occurence) {
				case UNIQUE: {
					count = 1;
					break;
				}
				case LOG: {
					count = count > 1 ? Math.round(Math.log(count)) + 1 : 1;
					break;
				}
				}

				for (int j = 0; j < count; ++j) {
					tokens.add(token);
				}
			}
		}
		return new SimpleTokenizedText(tokens.toArray(new String[tokens.size()]));
	}

	/**
	 * takes String defining the WordOccurence and returns the WordOccurence
	 * Enum
	 * 
	 * @param useWordOccurence
	 *            the String defining which WordOccurence to use
	 * @return the WordOccurence Enum
	 */
	public static WordOccurence getEnum(String useWordOccurence) {

		WordOccurence wordOccurence = null;

		switch (useWordOccurence.toUpperCase()) {
		case ("LOG"): {
			wordOccurence = WordOccurence.LOG;
			break;
		}
		case ("UNIQUE"): {
			wordOccurence = WordOccurence.UNIQUE;
			break;
		}
		default:
			LOGGER.error("Error while setting the WordOccurence. Returning null.");
		}

		return wordOccurence;

	}

}
