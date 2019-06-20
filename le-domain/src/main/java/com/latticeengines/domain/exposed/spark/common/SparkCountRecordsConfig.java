package com.latticeengines.domain.exposed.spark.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class SparkCountRecordsConfig extends SparkJobConfig {

    public static final String NAME = "sparkCount";

    @JsonProperty("globs")
    public String[] globs;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }
}
