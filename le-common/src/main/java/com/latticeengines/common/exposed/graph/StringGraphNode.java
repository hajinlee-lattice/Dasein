package com.latticeengines.common.exposed.graph;

public class StringGraphNode extends PrimitiveGraphNode<String> implements GraphNode {

    public StringGraphNode(String val) {
        super(val);
    }

    @Override
    public String toString() {
        return getVal();
    }

}
