package com.latticeengines.domain.exposed.query;

import java.util.Collection;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BucketRestriction extends Restriction {
    @JsonProperty("lhs")
    private ColumnLookup lhs;

    @JsonProperty("value")
    private int value;

    public BucketRestriction(ColumnLookup lhs, int value) {
        this.lhs = lhs;
        this.value = value;
    }

    public ColumnLookup getLhs() {
        return lhs;
    }

    public void setLhs(ColumnLookup lhs) {
        this.lhs = lhs;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
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
