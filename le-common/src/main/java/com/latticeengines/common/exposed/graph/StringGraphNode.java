package com.latticeengines.common.exposed.graph;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class StringGraphNode implements GraphNode {

    private final String val;
    private final Set<StringGraphNode> children;

    public StringGraphNode(String val) {
        this.val = val;
        this.children = new HashSet<>();
    }

    public String getVal() {
        return val;
    }

    public void addChild(StringGraphNode child) {
        children.add(child);
    }

    public Collection<StringGraphNode> getChildren() {
        return children;
    }

}
