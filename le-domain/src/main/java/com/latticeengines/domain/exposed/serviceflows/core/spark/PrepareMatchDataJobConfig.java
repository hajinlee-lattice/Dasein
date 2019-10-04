package com.latticeengines.domain.exposed.serviceflows.core.spark;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class PrepareMatchDataJobConfig extends SparkJobConfig {

    public static final String NAME = "prepareMatchData";

    @JsonProperty
    public List<String> matchFields;

    @JsonProperty
    public String matchGroupId;

    @Override
    public String getName() {
        return NAME;
    }
}
