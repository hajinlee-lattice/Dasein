package com.latticeengines.common.exposed.graph;

import java.util.Collection;
import java.util.Map;

import com.latticeengines.common.exposed.visitor.Visitable;

public interface GraphNode extends Visitable {

    Collection<? extends GraphNode> getChildren();
    
    Map<String, Collection<? extends GraphNode>> getChildMap();
}
