package com.latticeengines.domain.exposed.graph;

public class EdgeDeletionRequest {
    private String type;
    private String fromObjectID;
    private String fromObjectType;
    private String toObjectID;
    private String toObjectType;

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

}
