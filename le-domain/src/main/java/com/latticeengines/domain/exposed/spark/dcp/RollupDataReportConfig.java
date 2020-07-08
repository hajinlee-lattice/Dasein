package com.latticeengines.domain.exposed.spark.dcp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class RollupDataReportConfig extends SparkJobConfig {

    private static final String NAME = "RollupDataReport";

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
