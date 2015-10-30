package com.latticeengines.common.exposed.query;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Transformation {
    private TransformationType type;
    private Integer rangeStart;
    private Integer rangeEnd;

    public Transformation(TransformationType type, Integer rangeStart, Integer rangeEnd) {
        this.type = type;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
    }

    @JsonProperty("type")
    public TransformationType getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(TransformationType type) {
        this.type = type;
    }

    @JsonProperty("range_start")
    public Integer getRangeStart() {
        return rangeStart;
    }

    @JsonProperty("range_start")
    public void setRangeStart(Integer rangeStart) {
        this.rangeStart = rangeStart;
    }

    @JsonProperty("range_end")
    public Integer getRangeEnd() {
        return rangeEnd;
    }

    @JsonProperty("range_end")
    public void setRangeEnd(Integer rangeEnd) {
        this.rangeEnd = rangeEnd;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public Transformation() {
    }

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
