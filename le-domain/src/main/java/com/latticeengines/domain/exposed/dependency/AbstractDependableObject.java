package com.latticeengines.domain.exposed.dependency;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.latticeengines.common.exposed.graph.GraphNode;

public abstract class AbstractDependableObject implements Dependable, GraphNode {

    @Override
    public Collection<? extends GraphNode> getChildren() {
        return getDependencies();
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("dependencies", getChildren());
        return map;
    }


}
