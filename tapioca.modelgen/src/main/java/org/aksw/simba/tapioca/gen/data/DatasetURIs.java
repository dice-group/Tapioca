/**
 * tapioca.modelgen - ${project.description}
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
 * This file is part of tapioca.modelgen.
 *
 * tapioca.modelgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.modelgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.modelgen.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.aksw.simba.tapioca.gen.data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.aksw.simba.topicmodeling.utils.doc.AbstractSimpleDocumentProperty;

import com.carrotsearch.hppc.ObjectOpenHashSet;

public class DatasetURIs extends AbstractSimpleDocumentProperty<ObjectOpenHashSet<String>> implements Externalizable {

    public DatasetURIs() {
        super(new ObjectOpenHashSet<String>());
    }

    public DatasetURIs(ObjectOpenHashSet<String> value) {
        super(value);
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        get().add((String[]) oi.readObject());
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        oo.writeObject(get().toArray(String.class));
    }

}
