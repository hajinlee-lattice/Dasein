package com.latticeengines.domain.exposed.spark.cdl;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class MergeProductConfig extends SparkJobConfig {

    public static final String NAME = "mergeProduct";

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
