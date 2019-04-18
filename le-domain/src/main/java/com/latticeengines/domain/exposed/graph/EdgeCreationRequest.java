package com.latticeengines.domain.exposed.graph;

import java.util.Map;

public class EdgeCreationRequest {
    private String type;
    private String fromObjectID;
    private String fromObjectType;
    private String toObjectID;
    private String toObjectType;
    private Map<String, String> properties;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFromObjectID() {
        return fromObjectID;
    }

    public void setFromObjectID(String fromObjectID) {
        this.fromObjectID = fromObjectID;
    }

    public String getFromObjectType() {
        return fromObjectType;
    }

    public void setFromObjectType(String fromObjectType) {
        this.fromObjectType = fromObjectType;
    }

    public String getToObjectID() {
        return toObjectID;
    }

    public void setToObjectID(String toObjectID) {
        this.toObjectID = toObjectID;
    }

    public String getToObjectType() {
        return toObjectType;
    }

    public void setToObjectType(String toObjectType) {
        this.toObjectType = toObjectType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
