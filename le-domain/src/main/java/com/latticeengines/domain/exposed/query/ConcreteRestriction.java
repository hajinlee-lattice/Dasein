package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class ConcreteRestriction extends Restriction {
    public ConcreteRestriction(boolean negate, Lookup lhs, ComparisonType relation, Lookup rhs) {
        this.negate = negate;
        this.lhs = lhs;
        this.relation = relation;
        this.rhs = rhs;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public ConcreteRestriction() {
    }

    @JsonProperty
    public boolean negate;

    @JsonProperty
    public Lookup lhs;

    @JsonProperty
    public ComparisonType relation;

    @JsonProperty
    public Lookup rhs;

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
