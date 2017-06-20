package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
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

    ConcreteRestriction() {
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

    @Override
    public Collection<? extends GraphNode> getChildren() {
        List<GraphNode> children = new ArrayList<>();
        children.add(lhs);
        children.add(rhs);
        return children;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("lhs", Collections.singletonList(lhs));
        map.put("rhs", Collections.singletonList(rhs));
        return map;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }
}
