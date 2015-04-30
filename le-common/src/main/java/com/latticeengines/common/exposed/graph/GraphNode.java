package com.latticeengines.common.exposed.graph;

import java.util.List;

import com.latticeengines.common.exposed.visitor.Visitable;

public interface GraphNode extends Visitable {

    List<? extends GraphNode> getChildren();
}
