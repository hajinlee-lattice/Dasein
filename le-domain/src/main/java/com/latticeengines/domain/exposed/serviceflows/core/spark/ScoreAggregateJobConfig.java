package com.latticeengines.domain.exposed.serviceflows.core.spark;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ScoreAggregateJobConfig extends SparkJobConfig {

    public static final String NAME = "scoreAggregateFlow";

    @JsonProperty("score_results_table_name")
    public String scoreResultsTableName;

    @JsonProperty("model_guid_field")
    public String modelGuidField;

    @JsonProperty("score_field_map")
    public Map<String, String> scoreFieldMap;

    @JsonProperty("expected_value")
    public Boolean expectedValue;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
