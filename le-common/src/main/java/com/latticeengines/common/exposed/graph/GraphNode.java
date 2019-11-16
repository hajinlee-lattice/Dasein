package com.latticeengines.common.exposed.graph;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.latticeengines.common.exposed.visitor.Visitable;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public interface GraphNode extends Visitable {

    // to determine if two GraphNodes of the same type (Class) should be the same Vertex in a graph
    default String getId() { return String.valueOf(System.identityHashCode(this)); }

    default Collection<? extends GraphNode> getChildren() {
        return Collections.emptyList();
    }

    default Map<String, Collection<? extends GraphNode>> getChildMap() {
        return Collections.emptyMap();
    }

    default void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

}
