package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.List;

public class ExistsRestriction extends Restriction {

    public ExistsRestriction(boolean negate, String association, List<Restriction> restrictions) {
        this.negate = negate;
        this.association = association;
        this.restrictions = restrictions;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public ExistsRestriction() {
    }

    @JsonProperty
    public boolean negate;

    @JsonProperty
    public String association;

    @JsonProperty
    public List<Restriction> restrictions;

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
    @SuppressWarnings("Unchecked")
    public List<GraphNode> getChildren() {
        return List.class.cast(restrictions);
    }
}
