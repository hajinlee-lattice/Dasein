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

public class ExistsRestriction extends Restriction {
    private boolean negate;
    private String association;
    private List<Restriction> restrictions;

    public ExistsRestriction(boolean negate, String association, List<Restriction> restrictions) {
        this.negate = negate;
        this.association = association;
        this.restrictions = restrictions;
    }

    @JsonProperty("negate")
    public boolean isNegate() {
        return negate;
    }

    @JsonProperty("negate")
    public void setNegate(boolean negate) {
        this.negate = negate;
    }

    @JsonProperty("association")
    public String getAssociation() {
        return association;
    }

    @JsonProperty("association")
    public void setAssociation(String association) {
        this.association = association;
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
    public ExistsRestriction() {
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
