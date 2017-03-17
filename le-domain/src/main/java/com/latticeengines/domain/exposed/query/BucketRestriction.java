package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class BucketRestriction extends Restriction {
    @JsonProperty("lhs")
    private ColumnLookup lhs;

    @JsonProperty("range")
    private BucketRange range;

    public BucketRestriction(ColumnLookup lhs, BucketRange range) {
        this.lhs = lhs;
        this.range = range;
    }

    public BucketRestriction() {
    }

    public ColumnLookup getLhs() {
        return lhs;
    }

    public void setLhs(ColumnLookup lhs) {
        this.lhs = lhs;
    }

    public BucketRange getRange() {
        return range;
    }

    public void setRange(BucketRange range) {
        this.range = range;
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        List<GraphNode> children = new ArrayList<>();
        children.add(lhs);
        return children;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("lhs", Collections.singletonList(lhs));
        return map;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }
}
