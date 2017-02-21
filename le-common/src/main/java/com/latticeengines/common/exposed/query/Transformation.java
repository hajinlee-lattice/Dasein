package com.latticeengines.common.exposed.query;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Transformation {
    @JsonProperty("type")
    private TransformationType type;

    @JsonProperty("range_start")
    private Integer rangeStart;

    @JsonProperty("range_end")
    private Integer rangeEnd;

    public Transformation(TransformationType type, Integer rangeStart, Integer rangeEnd) {
        this.type = type;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
    }

    public TransformationType getType() {
        return type;
    }

    public void setType(TransformationType type) {
        this.type = type;
    }

    public Integer getRangeStart() {
        return rangeStart;
    }

    public void setRangeStart(Integer rangeStart) {
        this.rangeStart = rangeStart;
    }

    public Integer getRangeEnd() {
        return rangeEnd;
    }

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
