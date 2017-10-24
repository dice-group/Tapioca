/**
 * tapioca.core - ${project.description}
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
 * This file is part of tapioca.core.
 *
 * tapioca.core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.core.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.extraction.voidex;

import java.util.ArrayList;
import java.util.List;

public class DatasetDescription {
    public String uri;
    public String title;
    public String description;
    public List<String[]> keyValuePairs = new ArrayList<String[]>();
    public List<DatasetDescription> subsets;
    public long triples;

    public DatasetDescription(String datasetUri) {
        this.uri = datasetUri;
    }

    public DatasetDescription(String uri, String title) {
        this.uri = uri;
        this.title = title;
    }

    public DatasetDescription(String uri, String title, String description) {
        this.uri = uri;
        this.title = title;
        this.description = description;
    }

    public void addSubset(DatasetDescription subset) {
        if (subsets == null) {
            subsets = new ArrayList<DatasetDescription>();
        }
        subsets.add(subset);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((uri == null) ? 0 : uri.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DatasetDescription other = (DatasetDescription) obj;
        if (uri == null) {
            if (other.uri != null)
                return false;
        } else if (!uri.equals(other.uri))
            return false;
        return true;
    }

}