package com.latticeengines.domain.exposed.spark.stats;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class FindChangedProfileConfig extends SparkJobConfig {

    public static final String NAME = "findChangedProfile";

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public int getNumTargets() {
        return 0;
    }

}
