package com.latticeengines.common.exposed.graph;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class PrimitiveGraphNode<T> implements GraphNode {

    private final T val;
    private final Set<PrimitiveGraphNode<T>> children;

    PrimitiveGraphNode(T val) {
        this.val = val;
        this.children = new HashSet<>();
    }

    public T getVal() {
        return val;
    }

    public void addChild(PrimitiveGraphNode<T> child) {
        children.add(child);
    }

    public Collection<PrimitiveGraphNode<T>> getChildren() {
        return children;
    }

}
