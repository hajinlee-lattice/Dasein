package com.latticeengines.domain.exposed.spark;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public final class ScriptJobConfig extends SparkJobConfig {

    public static final String NAME = "script";

    @JsonProperty("Params")
    private JsonNode params;

    @JsonProperty("NumTargets")
    private int numTargets = 1;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public JsonNode getParams() {
        return params;
    }

    public void setParams(JsonNode params) {
        this.params = params;
    }

    @Override
    public int getNumTargets() {
        return numTargets;
    }

    public void setNumTargets(int numTargets) {
        this.numTargets = numTargets;
    }
}
