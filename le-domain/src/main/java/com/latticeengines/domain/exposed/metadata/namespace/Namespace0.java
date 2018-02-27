package com.latticeengines.domain.exposed.metadata.namespace;

import java.io.Serializable;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Namespace0 extends Namespace {

    public Serializable[] coords() {
        return new Serializable[]{};
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(29, 61).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && (obj == this || obj.getClass() == getClass());
    }

}
