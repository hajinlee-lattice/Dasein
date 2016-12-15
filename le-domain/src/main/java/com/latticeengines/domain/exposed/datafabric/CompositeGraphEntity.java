package com.latticeengines.domain.exposed.datafabric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public class CompositeGraphEntity implements GraphNode {

    CompositeFabricEntity entity;
    String name;

    List<CompositeGraphEntity> children;
    Map<String, Collection<CompositeGraphEntity>> childMap;

    public CompositeGraphEntity(String name, CompositeFabricEntity entity) {
        this.entity = entity;
        this.name = name;
        children = new ArrayList<CompositeGraphEntity>();
        childMap = new HashMap<String, Collection<CompositeGraphEntity>>();
    }

    @Override
    public Collection<CompositeGraphEntity> getChildren() {
        return children;
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return null;
    }

    public CompositeFabricEntity getEntity() {
        return entity;
    }

    public String getName() {
        return name;
    }

    public Collection<CompositeGraphEntity> getChild(String name) {
        return childMap.get(name);
    }

    public void addChild(CompositeGraphEntity child) {
        Collection<CompositeGraphEntity> collection = childMap.get(child.getName());
        if (collection == null) {
            collection = new ArrayList<CompositeGraphEntity>();
            childMap.put(child.getName(), collection);
        }
        collection.add(child);
        children.add(child);
    }

    public void accept(Visitor visitor, VisitorContext context) {
        return;
    }
}
