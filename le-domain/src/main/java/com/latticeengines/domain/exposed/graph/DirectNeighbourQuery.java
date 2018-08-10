package com.latticeengines.domain.exposed.graph;

import java.util.List;

public class DirectNeighbourQuery {
    private List<String> objectIds;
    private List<String> objectTypes;

    public List<String> getObjectIds() {
        return objectIds;
    }

    public void setObjectIds(List<String> objectIds) {
        this.objectIds = objectIds;
    }

    public List<String> getObjectTypes() {
        return objectTypes;
    }

    public void setObjectTypes(List<String> objectTypes) {
        this.objectTypes = objectTypes;
    }

}
