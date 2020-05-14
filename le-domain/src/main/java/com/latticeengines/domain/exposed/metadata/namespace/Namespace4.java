package com.latticeengines.domain.exposed.metadata.namespace;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Namespace4<T1 extends Serializable, T2 extends Serializable, T3 extends Serializable, T4 extends Serializable>
        extends Namespace3<T1, T2, T3> {

    final T4 coord4;

    Namespace4(T1 coord1, T2 coord2, T3 coord3, T4 coord4) {
        super(coord1, coord2, coord3);
        this.coord4 = coord4;
    }

    public Serializable[] coords() {
        return new Serializable[] { coord1, coord2, coord3, coord4 };
    }

    public T4 getCoord4() {
        return coord4;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(29, 61) //
                .append(coord1) //
                .append(coord2) //
                .append(coord3) //
                .append(coord4) //
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
        Namespace4<?, ?, ?, ?> rhs = (Namespace4<?, ?, ?, ?>) obj;
        return new EqualsBuilder() //
                .appendSuper(super.equals(obj)) //
                .append(coord1, rhs.coord1) //
                .append(coord2, rhs.coord2) //
                .append(coord3, rhs.coord3) //
                .append(coord4, rhs.coord4) //
                .isEquals();
    }

}
