package com.latticeengines.common.exposed.graph;

import java.util.Collection;
import java.util.Map;

import com.latticeengines.common.exposed.visitor.Visitable;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public interface GraphNode extends Visitable {

    Collection<? extends GraphNode> getChildren();

    Map<String, Collection<? extends GraphNode>> getChildMap();

    default void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }
}
