package com.latticeengines.telepath.entities;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.telepath.relations.BaseRelation;

public interface Entity {

    String __NAMESPACE = "__nameSpace";
    String __ENTITY_ID = "__entityId";
    String __ENTITY_TYPE = "__type";
    String __TOMBSTONE = "__tombstone"; // boolean attr indicating an obj is active

    String __ID_PLACEHOLDER = "__tempId"; // temporary id for entities pending creation

    String getEntityType();

    String getEntityId();

    Map<String, Object> getProps();

    @JsonIgnore
    String getEntityHash(); // for checking if a node for an entity already created in graph

    @JsonIgnore
    List<Pair<? extends BaseEntity, ? extends BaseRelation>> getEntityRelations(); // for meta graph

    /**
     * Extract properties needed for validation from graph vertex.
     *
     * @param v:
     *            vertex
     */
    void extractPropsFromVertex(Vertex v);

    void updateVertex(Vertex vertex);

    Vertex createVertexAndAddToGraph(GraphTraversalSource g);
}
