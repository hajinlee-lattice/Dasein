package com.latticeengines.domain.exposed.query;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConcreteRestriction extends Restriction {
    @JsonProperty("negate")
    private boolean negate;
    @JsonProperty("lhs")
    private Lookup lhs;
    @JsonProperty("relation")
    private ComparisonType relation;
    @JsonProperty("rhs")
    private Lookup rhs;

    public ConcreteRestriction(boolean negate, Lookup lhs, ComparisonType relation, Lookup rhs) {
        this.negate = negate;
        this.lhs = lhs;
        this.relation = relation;
        this.rhs = rhs;
    }

    public ConcreteRestriction() {
    }

    public Lookup getLhs() {
        return lhs;
    }

    public void setLhs(Lookup lhs) {
        this.lhs = lhs;
    }

    public ComparisonType getRelation() {
        return relation;
    }

    public void setRelation(ComparisonType relation) {
        this.relation = relation;
    }

    public Lookup getRhs() {
        return rhs;
    }

    public void setRhs(Lookup rhs) {
        this.rhs = rhs;
    }

    public boolean getNegate() {
        return negate;
    }

    public void setNegate(boolean negate) {
        this.negate = negate;
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
