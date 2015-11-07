package com.latticeengines.scoring.util;

import java.util.HashMap;

import com.fasterxml.jackson.databind.JsonNode;

public class LocalizedFiles {
    private JsonNode datatype;
    // key: modelGuid, value: model contents
    private HashMap<String, JsonNode> models;

    public JsonNode getDatatype() {
        return this.datatype;
    }

    public HashMap<String, JsonNode> getModels() {
        return this.models;
    }

    public void setModels(HashMap<String, JsonNode> models) {
        this.models = models;
    }

    public void setDatatype(JsonNode datatype) {
        this.datatype = datatype;
    }

}
