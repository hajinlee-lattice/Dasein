package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class MergeLatticeAccountConfig extends SparkJobConfig {

    public static final String NAME = "mergeLatticeAccount";

    @JsonProperty("MergeMode")
    private String mergeMode;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public String getMergeMode() {
        return mergeMode;
    }

    public void setMergeMode(String mergeMode) {
        this.mergeMode = mergeMode;
    }
}
