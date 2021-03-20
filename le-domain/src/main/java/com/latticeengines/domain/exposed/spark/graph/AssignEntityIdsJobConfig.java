package com.latticeengines.domain.exposed.spark.graph;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class AssignEntityIdsJobConfig extends SparkJobConfig {

    public static final String NAME = "assignEntityIdsJob";

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 3;
    }
}
