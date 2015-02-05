package com.latticeengines.scoringharness.cloudmodel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class BaseCloudUpdate {

    public String objectType = null;
    public String action = null;
    public ArrayNode objects = null;

    public BaseCloudUpdate(String objectType, String action) {
        this.objectType = objectType;
        this.action = action;
        this.objects = new ObjectMapper().createArrayNode();
    }

    public BaseCloudUpdate(String objectType, String action, ArrayNode objects) {
        this.objectType = objectType;
        this.action = action;
        this.objects = objects;
    }

    public void addRow(JsonNode jsonRow) {
        objects.add(jsonRow);
    }
}
