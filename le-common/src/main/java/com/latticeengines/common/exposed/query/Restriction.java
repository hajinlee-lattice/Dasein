package com.latticeengines.common.exposed.query;


import java.util.ArrayList;
import java.util.List;

import com.latticeengines.common.exposed.graph.GraphNode;

public abstract class Restriction implements GraphNode {

    @Override
    public List<GraphNode> getChildren() {
        return new ArrayList<GraphNode>();
    }
}
