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
package org.aksw.simba.tapioca.cores.data;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

/**
 * This class represents the VoID information.
 */
public abstract class VoidInformation {
    public String uri;
    public int count;

    public boolean isComplete() {
        return uri != null;
    }

    /**
     * An abstract function for adding the counted information.
     * @param countedClasses The counted classes
     * @param countedProperties The counted properties
     */
    public abstract void addToCount(ObjectIntOpenHashMap<String> countedClasses,
            ObjectIntOpenHashMap<String> countedProperties);
    
    /**
     * Cast to ClassDescription
     * @return Class description
     */
    public ClassDescription toClass() {
    	ClassDescription classDesc = new ClassDescription();
    	classDesc.count = this.count;
    	classDesc.uri = this.uri;
    	return classDesc;
    }

    /**
     * Cast to PropertyDescription
     * @return Property description
     */
    public PropertyDescription toProperty() {
    	PropertyDescription propertyDesc = new PropertyDescription();
    	propertyDesc.count = this.count;
    	propertyDesc.uri = this.uri;
    	return propertyDesc;
    }
}
