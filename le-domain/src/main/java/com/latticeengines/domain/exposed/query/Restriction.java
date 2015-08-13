package com.latticeengines.domain.exposed.query;


import com.latticeengines.common.exposed.graph.GraphNode;

import java.util.ArrayList;
import java.util.List;

public abstract class Restriction implements GraphNode {
    @Override
    public List<GraphNode> getChildren() {
        return new ArrayList<GraphNode>();
    }
}
