package com.latticeengines.common.exposed.query;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SingleReferenceLookup extends Lookup {
    private Object reference;
    private ReferenceInterpretation interpretation;

    public SingleReferenceLookup(Object reference, ReferenceInterpretation interpretation) {
        this.reference = reference;
        this.interpretation = interpretation;
    }

    @JsonProperty("reference")
    public Object getReference() {
        return reference;
    }

    @JsonProperty("reference")
    public void setReference(Object reference) {
        this.reference = reference;
    }

    @JsonProperty("interpretation")
    public ReferenceInterpretation getInterpretation() {
        return interpretation;
    }

    @JsonProperty("interpretation")
    public void setInterpretation(ReferenceInterpretation interpretation) {
        this.interpretation = interpretation;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public SingleReferenceLookup() {
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
