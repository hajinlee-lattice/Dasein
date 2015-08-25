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
