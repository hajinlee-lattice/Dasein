package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;

/**
 * This restriction is only needed by front end
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class BucketRestriction extends Restriction {

    @JsonIgnore
    private AttributeLookup attr;

    @JsonProperty("bkt")
    private Bucket bkt;

    public BucketRestriction(AttributeLookup attr, Bucket bkt) {
        this.attr = attr;
        this.bkt = bkt;
    }

    public BucketRestriction() {
    }

    public AttributeLookup getAttr() {
        return attr;
    }

    public void setAttr(AttributeLookup attr) {
        this.attr = attr;
    }

    public Bucket getBkt() {
        return bkt;
    }

    public void setBkt(Bucket bkt) {
        this.bkt = bkt;
    }

    // to simplify UI json. other restrictions does not need this.
    @JsonProperty("attr")
    private String getAttrAsString() {
        return attr == null ? null : attr.toString();
    }

    @JsonProperty("attr")
    private void setAttrViaString(String attr) {
        this.attr = AttributeLookup.fromString(attr);
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        List<GraphNode> children = new ArrayList<>();
        children.add(attr);
        return children;
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("attr", Collections.singletonList(attr));
        return map;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

}
