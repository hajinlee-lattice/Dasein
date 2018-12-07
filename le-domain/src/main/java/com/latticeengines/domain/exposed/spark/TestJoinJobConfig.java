package com.latticeengines.domain.exposed.spark;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class TestJoinJobConfig extends SparkJobConfig {

    public static final String NAME = "testJoin";

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 2;
    }

}
