package com.latticeengines.domain.exposed.metadata.namespace;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Namespace2<T1 extends Serializable, T2 extends Serializable> extends Namespace1<T1> {

    final T2 coord2;

    Namespace2(T1 coord1, T2 coord2) {
        super(coord1);
        this.coord2 = coord2;
    }

    public Serializable[] coords() {
        return new Serializable[] { coord1, coord2 };
    }

    public T2 getCoord2() {
        return coord2;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(29, 61) //
                .append(coord1) //
                .append(coord2) //
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
        Namespace2<?, ?> rhs = (Namespace2<?, ?>) obj;
        return new EqualsBuilder() //
                .appendSuper(super.equals(obj)) //
                .append(coord1, rhs.coord1) //
                .append(coord2, rhs.coord2) //
                .isEquals();
    }

}
