package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class MergeCSVConfig extends SparkJobConfig {

    private static final long serialVersionUID = 9165843065652451988L;

    public static final String NAME = "mergeCSV";

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
