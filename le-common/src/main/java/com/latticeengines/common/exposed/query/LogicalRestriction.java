package com.latticeengines.common.exposed.query;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public class LogicalRestriction extends Restriction {
    private Connective connective;
    private List<Restriction> restrictions;

    public LogicalRestriction(Connective connective, List<Restriction> restrictions) {
        this.connective = connective;
        this.restrictions = restrictions;
    }

    @JsonProperty("connective")
    public Connective getConnective() {
        return connective;
    }

    @JsonProperty("connective")
    public void setConnective(Connective connective) {
        this.connective = connective;
    }

    @JsonProperty("restrictions")
    public List<Restriction> getRestrictions() {
        return restrictions;
    }

    @JsonProperty("restrictions")
    public void setRestrictions(List<Restriction> restrictions) {
        this.restrictions = restrictions;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public LogicalRestriction() {
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
    @SuppressWarnings("unchecked")
    public List<GraphNode> getChildren() {
        return List.class.cast(restrictions);
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        // TODO Auto-generated method stub
        return null;
    }
}
