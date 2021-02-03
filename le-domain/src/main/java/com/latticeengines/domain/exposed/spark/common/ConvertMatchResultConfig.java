package com.latticeengines.domain.exposed.spark.common;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ConvertMatchResultConfig extends SparkJobConfig {

    public static final String NAME = "convertMatchResult";

    @JsonProperty("DisplayNames")
    private Map<String, String> displayNames;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public Map<String, String> getDisplayNames() {
        return displayNames;
    }

    public void setDisplayNames(Map<String, String> displayNames) {
        this.displayNames = displayNames;
    }
}
