package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class Transformation {
    public Transformation(TransformationType type, Integer rangeStart, Integer rangeEnd) {
        this.type = type;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public Transformation() {
    }

    @JsonProperty
    public TransformationType type;

    @JsonProperty
    public Integer rangeStart;

    @JsonProperty
    public Integer rangeEnd;

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
