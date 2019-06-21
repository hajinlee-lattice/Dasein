package com.latticeengines.domain.exposed.spark.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CountAvroGlobsConfig extends SparkJobConfig {

    public static final String NAME = "sparkCount";

    @JsonProperty("avroGlobs")
    public String[] avroGlobs;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 0;
    }

}
