package com.latticeengines.domain.exposed.graph;

import java.util.List;
import java.util.Map;

public class Vertex {
    private String vertexId;
    private String objectId;
    private String idPrefix;
    private String name;
    private List<String> labels;
    private String vertexType;
    private String behaviorOnDepCheckTraversal;
    private Map<String, String> properties;

    public String getVertexId() {
        return vertexId;
    }

    public void setVertexId(String vertexId) {
        this.vertexId = vertexId;
    }

    public String getObjectId() {
        return objectId;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }

    public String getIdPrefix() {
        return idPrefix;
    }

    public void setIdPrefix(String idPrefix) {
        this.idPrefix = idPrefix;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getLabels() {
        return labels;
    }

    public void setLabels(List<String> labels) {
        this.labels = labels;
    }

    public String getVertexType() {
        return vertexType;
    }

    public void setVertexType(String vertexType) {
        this.vertexType = vertexType;
    }

    public String getBehaviorOnDepCheckTraversal() {
        return behaviorOnDepCheckTraversal;
    }

    public void setBehaviorOnDepCheckTraversal(String behaviorOnDepCheckTraversal) {
        this.behaviorOnDepCheckTraversal = behaviorOnDepCheckTraversal;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
