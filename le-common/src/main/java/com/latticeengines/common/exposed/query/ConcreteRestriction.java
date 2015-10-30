package com.latticeengines.common.exposed.query;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public class ConcreteRestriction extends Restriction {
    private boolean negate;
    private Lookup lhs;
    private ComparisonType relation;
    private Lookup rhs;

    public ConcreteRestriction(boolean negate, Lookup lhs, ComparisonType relation, Lookup rhs) {
        this.negate = negate;
        this.lhs = lhs;
        this.relation = relation;
        this.rhs = rhs;
    }

    @JsonProperty("lhs")
    public Lookup getLhs() {
        return lhs;
    }

    @JsonProperty("lhs")
    public void setLhs(Lookup lhs) {
        this.lhs = lhs;
    }

    @JsonProperty("relation")
    public ComparisonType getRelation() {
        return relation;
    }

    @JsonProperty("relation")
    public void setRelation(ComparisonType relation) {
        this.relation = relation;
    }

    @JsonProperty("rhs")
    public Lookup getRhs() {
        return rhs;
    }

    @JsonProperty("rhs")
    public void setRhs(Lookup rhs) {
        this.rhs = rhs;
    }

    @JsonProperty("negate")
    public boolean isNegate() {
        return negate;
    }

    @JsonProperty("negate")
    public void setNegate(boolean negate) {
        this.negate = negate;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public ConcreteRestriction() {
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

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        // TODO Auto-generated method stub
        return null;
    }
}
