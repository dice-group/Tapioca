/**
 * This file is part of tapioca.indexgenerator.
 *
 * tapioca.indexgenerator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.indexgenerator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.indexgenerator.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.indexgenerator.docgen.data;

import java.util.Date;

/**
 * Simulate the StatResult data. Variables for the file ID, the URI and the time
 * stamp.
 *
 */
public class StatResult {

	// -------------------------------------------------------------------------
	// ------------------ Variables --------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * The file ID.
	 */
	public int id;

	/**
	 * The file URI.
	 */
	public String uri;

	/**
	 * The data set URI.
	 */
	public String datasetUri;

	/**
	 * The time stamp.
	 */
	public Date timestamp;

	// -------------------------------------------------------------------------
	// ------------------ Methods ----------------------------------------------
	// -------------------------------------------------------------------------

	/**
	 * Constructor. Create a StatResult entry with ID and URI.
	 * 
	 * @param id
	 *            The ID of the file.
	 * @param uri
	 *            The URI of the file.
	 */
	public StatResult(int id, String uri) {
		this.id = id;
		this.uri = uri;
	}

	/**
	 * 
	 * @return The ID of the file.
	 */
	public int getId() {
		return id;
	}

	/**
	 * Set the ID of the file.
	 * 
	 * @param id
	 *            The ID of the file.
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 *
	 * @return The URI of the file.
	 */
	public String getUri() {
		return uri;
	}

	/**
	 * Set the URI of the file.
	 * 
	 * @param uri
	 *            The URI of the file.
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}

	/**
	 * 
	 * @return The URI of the data set.
	 */
	public String getDatasetUri() {
		return datasetUri;
	}

	/**
	 * Set the data set URI.
	 * 
	 * @param datasetUri
	 *            The data set URI.
	 */
	public void setDatasetUri(String datasetUri) {
		this.datasetUri = datasetUri;
	}

	/**
	 * 
	 * @return The time stamp.
	 */
	public Date getTimestamp() {
		return timestamp;
	}

	/**
	 * Set the time stamp.
	 * 
	 * @param timestamp
	 *            The time stamp.
	 */
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

}
