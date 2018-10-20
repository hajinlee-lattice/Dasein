package com.latticeengines.domain.exposed.graph;

import java.util.List;
import java.util.Map;

public class VertexCreationRequest {
    private String objectId;
    private String type;
    private List<String> labels;
    private Map<String, String> properties;
    private Map<String, Map<String, Map<String, String>>> outgoingEdgesToVertices;
    private Map<String, String> outgoingVertexTypes;

    public String getObjectId() {
        return objectId;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getLabels() {
        return labels;
    }

    public void setLabels(List<String> labels) {
        this.labels = labels;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Map<String, Map<String, Map<String, String>>> getOutgoingEdgesToVertices() {
        return outgoingEdgesToVertices;
    }

    public void setOutgoingEdgesToVertices(
            Map<String, Map<String, Map<String, String>>> outgoingEdgesToVertices) {
        this.outgoingEdgesToVertices = outgoingEdgesToVertices;
    }

    public Map<String, String> getOutgoingVertexTypes() {
        return outgoingVertexTypes;
    }

    public void setOutgoingVertexTypes(Map<String, String> outgoingVertexTypes) {
        this.outgoingVertexTypes = outgoingVertexTypes;
    }

}
