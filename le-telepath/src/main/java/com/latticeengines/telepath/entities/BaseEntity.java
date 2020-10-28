package com.latticeengines.telepath.entities;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.telepath.relations.BaseRelation;

public abstract class BaseEntity implements Entity {

    public abstract String getEntityType();

    @JsonIgnore
    public abstract List<Pair<? extends BaseEntity, ? extends BaseRelation>> getEntityRelations();

    public abstract void extractPropsFromObj(Object obj);

    public Vertex createVertexAndAddToGraph(GraphTraversalSource g) {
        String entityType = getEntityType();
        if (g.V().has(entityType, __ENTITY_TYPE, entityType).has(entityType, __ENTITY_ID, getProps().get(__ENTITY_ID))
                .hasNext()) {
            return g.V().has(entityType, __ENTITY_TYPE, entityType)
                    .has(entityType, __ENTITY_ID, getProps().get(__ENTITY_ID)).next();
        } else {
            getProps().putIfAbsent(__ENTITY_ID, __ID_PLACEHOLDER);
            GraphTraversal<Vertex, Vertex> steps = g.addV(entityType).property(__ENTITY_TYPE, entityType);
            for (Map.Entry<String, Object> entry : getProps().entrySet()) {
                steps = steps.property(entry.getKey(), entry.getValue());
            }
            return steps.next();
        }
    }

    /**
     * Extract properties needed for validation from graph vertex.
     *
     * @param v:
     *            vertex
     */
    public void extractPropsFromVertex(Vertex v) {
        getProps().put(__TOMBSTONE, checkTombStoneVertiex(v));
        getProps().put(__ENTITY_ID, v.property(__ENTITY_ID).value().toString());
        extractSpecificPropsFromVertex(v);
    }

    protected void extractSpecificPropsFromVertex(Vertex v) {
    }

    protected Boolean checkTombStoneVertiex(Vertex v) {
        try {
            return (boolean) v.property(__TOMBSTONE).value();
        } catch (IllegalStateException e) { // no such property
            return false;
        }
    }

    @JsonIgnore
    public String getEntityHash() {
        String pid = getProps().get(__ENTITY_ID).toString();
        String entityType = getEntityType();
        String namespace = getProps().getOrDefault(__NAMESPACE, "").toString();
        return HashUtils.getShortHash(pid + entityType + namespace);
    }

    @JsonIgnore
    public String getEntityId() {
        return getProps().get(__ENTITY_ID).toString();
    }

    public void updateVertex(Vertex vertex) {
        throw new IllegalStateException("Not Implemented");
    }
}
