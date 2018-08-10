package com.latticeengines.domain.exposed.graph;

import java.util.Map;

public class Edge {
    private String edgeId;
    private String idPrefix;
    private String fromVertexId;
    private String toVertexId;
    private String label;
    private String type;
    private String behaviorOnDepCheckTraversal;
    private String behaviorOnDeleteOfInVertex;
    private Map<String, String> properties;

    public String getEdgeId() {
        return edgeId;
    }

    public void setEdgeId(String edgeId) {
        this.edgeId = edgeId;
    }

    public String getIdPrefix() {
        return idPrefix;
    }

    public void setIdPrefix(String idPrefix) {
        this.idPrefix = idPrefix;
    }

    public String getFromVertexId() {
        return fromVertexId;
    }

    public void setFromVertexId(String fromVertexId) {
        this.fromVertexId = fromVertexId;
    }

    public String getToVertexId() {
        return toVertexId;
    }

    public void setToVertexId(String toVertexId) {
        this.toVertexId = toVertexId;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getBehaviorOnDepCheckTraversal() {
        return behaviorOnDepCheckTraversal;
    }

    public void setBehaviorOnDepCheckTraversal(String behaviorOnDepCheckTraversal) {
        this.behaviorOnDepCheckTraversal = behaviorOnDepCheckTraversal;
    }

    public String getBehaviorOnDeleteOfInVertex() {
        return behaviorOnDeleteOfInVertex;
    }

    public void setBehaviorOnDeleteOfInVertex(String behaviorOnDeleteOfInVertex) {
        this.behaviorOnDeleteOfInVertex = behaviorOnDeleteOfInVertex;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
