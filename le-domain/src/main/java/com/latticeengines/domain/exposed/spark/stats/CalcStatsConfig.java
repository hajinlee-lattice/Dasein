package com.latticeengines.domain.exposed.spark.stats;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CalcStatsConfig extends SparkJobConfig {

    public static final String NAME = "calcStats";

    // when defined, ignore attributes outside of this list
    @JsonProperty("includeAttrs")
    private List<String> includeAttrs;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public List<String> getIncludeAttrs() {
        return includeAttrs;
    }

    public void setIncludeAttrs(List<String> includeAttrs) {
        this.includeAttrs = includeAttrs;
    }
}
