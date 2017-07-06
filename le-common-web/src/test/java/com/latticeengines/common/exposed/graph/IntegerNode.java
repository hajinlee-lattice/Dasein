package com.latticeengines.common.exposed.graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

class IntegerNode implements GraphNode {

    public Set<IntegerNode> children = new HashSet<>();
    public Integer value;

    public IntegerNode(Integer value) { this.value = value; }

    @Override
    public Collection<IntegerNode> getChildren() { return children; }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

    @Override
    public boolean equals(Object that) {
        return that.getClass().equals(IntegerNode.class)
                && this.value.equals(((IntegerNode) that).value);
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("integers", children);
        return map;
    }

}

