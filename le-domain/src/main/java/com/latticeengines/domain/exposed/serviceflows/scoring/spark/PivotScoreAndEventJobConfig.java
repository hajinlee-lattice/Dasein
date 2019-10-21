package com.latticeengines.domain.exposed.serviceflows.scoring.spark;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class PivotScoreAndEventJobConfig extends SparkJobConfig {

    private static final long serialVersionUID = 7122317497153654527L;
    public static final String NAME = "pivotScoreAndEvent";

    @JsonProperty("event_column")
    public String eventColumn;

    @JsonProperty("avg_scores")
    public Map<String, Double> avgScores = new HashMap<>();

    @JsonProperty("score_field_map")
    public Map<String, String> scoreFieldMap = new HashMap<>();

    @JsonProperty("score_derivation_map")
    public Map<String, String> scoreDerivationMap = new HashMap<>();

    @JsonProperty("fit_function_parameters_map")
    public Map<String, String> fitFunctionParametersMap = new HashMap<>();

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
