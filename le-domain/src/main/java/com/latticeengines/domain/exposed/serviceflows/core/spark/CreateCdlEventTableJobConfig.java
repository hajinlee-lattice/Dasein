package com.latticeengines.domain.exposed.serviceflows.core.spark;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CreateCdlEventTableJobConfig extends SparkJobConfig {

    public static final String NAME = "createCdlEventTableFlow";

    @JsonProperty("event_column")
    public String eventColumn;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
