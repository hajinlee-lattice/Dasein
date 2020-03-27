package com.latticeengines.domain.exposed.spark.dcp;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class SplitImportMatchResultConfig extends SparkJobConfig {

    public static final String NAME = "splitImportMatchResult";

    // (attr -> dispName) mapping for attributes in the accepted split
    @JsonProperty("AcceptedAttrs")
    private Map<String, String> acceptedAttrsMap;

    // (attr -> dispName) mapping for attributes in the rejected split
    @JsonProperty("RejectedAttrs")
    private Map<String, String> rejectedAttrsMap;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 2;
    }

    public Map<String, String> getAcceptedAttrsMap() {
        return acceptedAttrsMap;
    }

    public void setAcceptedAttrsMap(Map<String, String> acceptedAttrsMap) {
        this.acceptedAttrsMap = acceptedAttrsMap;
    }

    public Map<String, String> getRejectedAttrsMap() {
        return rejectedAttrsMap;
    }

    public void setRejectedAttrsMap(Map<String, String> rejectedAttrsMap) {
        this.rejectedAttrsMap = rejectedAttrsMap;
    }
}
