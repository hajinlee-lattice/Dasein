package com.latticeengines.scoring.util;

import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;

public class LocalizedFiles {
    // key: modelGuid, value: model contents
    private Map<String, JsonNode> models;

    public Map<String, JsonNode> getModels() {
        return this.models;
    }

    public void setModels(Map<String, JsonNode> models) {
        this.models = models;
    }

}
