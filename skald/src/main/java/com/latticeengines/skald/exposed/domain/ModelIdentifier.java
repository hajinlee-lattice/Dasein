package com.latticeengines.skald.exposed.domain;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class ModelIdentifier {
    public ModelIdentifier(String name, int version) {
        this.name = name;
        this.version = version;
    }

    // Serialization Constructor.
    public ModelIdentifier() {
    }

    public String name;

    public int version;

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
