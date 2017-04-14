package com.latticeengines.domain.exposed.dependency;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.domain.exposed.metadata.DependableObject;

public abstract class AbstractDependableObject implements Dependable, GraphNode {
    @JsonProperty("dependencies")
    @Transient
    private List<DependableObject> dependencies = new ArrayList<>();

    @Override
    public String getType() {
        return getClass().getTypeName();
    }

    @Override
    public List<DependableObject> getDependencies() {
        return dependencies;
    }

    @Override
    public void setDependencies(List<DependableObject> dependencies) {
        this.dependencies = dependencies;
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        return dependencies;
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("dependencies", getChildren());
        return map;
    }

    public void addDependency(DependableObject child) {
        dependencies.add(child);
    }
}
