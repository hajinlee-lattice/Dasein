package com.latticeengines.domain.exposed.metadata.namespace;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Namespace1<T extends Serializable> extends Namespace {

    final T coord1;

    Namespace1(T coord1) {
        this.coord1 = coord1;
    }

    public Serializable[] coords() {
        return new Serializable[] { coord1 };
    }

    public T getCoord1() {
        return coord1;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(29, 61).append(coord1).toHashCode();
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
        Namespace1<?> rhs = (Namespace1<?>) obj;
        return new EqualsBuilder().appendSuper(super.equals(obj)).append(coord1, rhs.coord1)
                .isEquals();
    }

}
