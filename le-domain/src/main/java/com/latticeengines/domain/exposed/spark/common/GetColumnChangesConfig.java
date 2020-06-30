package com.latticeengines.domain.exposed.spark.common;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GetColumnChangesConfig extends SparkJobConfig {

    public static final String NAME = "getColumnChanges";

    @JsonProperty("includeAttrs")
    private List<String> includeAttrs;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 0;
    }

    public List<String> getIncludeAttrs() {
        return includeAttrs;
    }

    public void setIncludeAttrs(List<String> includeAttrs) {
        this.includeAttrs = includeAttrs;
    }

}
