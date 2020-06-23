package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ConcatenateAIRatingsConfig extends SparkJobConfig {

    public static final String NAME = "concatenateAIRatings";

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
