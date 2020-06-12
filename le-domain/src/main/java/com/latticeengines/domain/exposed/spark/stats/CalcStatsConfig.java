package com.latticeengines.domain.exposed.spark.stats;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CalcStatsConfig extends SparkJobConfig {

    public static final String NAME = "calcStats";

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
