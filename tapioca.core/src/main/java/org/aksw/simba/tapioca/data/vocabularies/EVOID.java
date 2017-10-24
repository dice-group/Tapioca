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
package org.aksw.simba.tapioca.data.vocabularies;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class EVOID {

    protected static final String uri = "http://aksw.org/ns/extended-void#";

    /**
     * returns the URI for this schema
     * 
     * @return the URI for this schema
     */
    public static String getURI() {
        return uri;
    }

    protected static final Property property(String local) {
        return ResourceFactory.createProperty(uri, local);
    }

    public static final Property specialClass = property("specialClass");
    public static final Property classPartition = property("classPartition");
    public static final Property entities = property("entities");

}
