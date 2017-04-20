package com.latticeengines.domain.exposed.dependency;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.common.exposed.graph.GraphNode;

public abstract class AbstractDependableObject implements Dependable, GraphNode {

    @Override
    @JsonIgnore
    @Transient
    public Collection<? extends GraphNode> getChildren() {
        return getDependencies();
    }

    @Override
    @JsonIgnore
    @Transient
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("dependencies", getChildren());
        return map;
    }

}
