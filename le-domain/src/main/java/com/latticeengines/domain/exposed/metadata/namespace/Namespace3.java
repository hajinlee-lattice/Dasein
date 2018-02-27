package com.latticeengines.domain.exposed.metadata.namespace;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Namespace3<T1 extends Serializable, T2 extends Serializable, T3 extends Serializable>
        extends Namespace2<T1, T2> {

    final T3 coord3;

    Namespace3(T1 coord1, T2 coord2, T3 coord3) {
        super(coord1, coord2);
        this.coord3 = coord3;
    }

    public Serializable[] coords() {
        return new Serializable[]{ coord1, coord2, coord3 };
    }

    public T3 getCoord3() {
        return coord3;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(29, 61) //
                .append(coord1) //
                .append(coord2) //
                .append(coord3) //
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        Namespace3 rhs = (Namespace3) obj;
        return new EqualsBuilder() //
                .appendSuper(super.equals(obj)) //
                .append(coord1, rhs.coord1) //
                .append(coord2, rhs.coord2) //
                .append(coord3, rhs.coord3) //
                .isEquals();
    }

}
