package com.latticeengines.domain.exposed.serviceflows.scoring.spark;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CalculateExpectedRevenuePercentileJobConfig extends SparkJobConfig {

    private static final long serialVersionUID = 8983386026501286368L;

    public static final String NAME = "calculateExpectedRevenuePercentileJobFlow";

    @JsonProperty("input_table_name")
    public String inputTableName;

    @JsonProperty("percentile_field_name")
    public String percentileFieldName;

    @JsonProperty("original_score_field_map")
    public Map<String, String> originalScoreFieldMap;

    @JsonProperty("model_guid_field")
    public String modelGuidField;

    @JsonProperty("percentile_lower_bound")
    public Integer percentileLowerBound;

    @JsonProperty("percentile_upper_bound")
    public Integer percentileUpperBound;

    @JsonProperty("target_score_derivation")
    public boolean targetScoreDerivation;

    @JsonProperty("fit_function_parameters_map")
    public Map<String, String> fitFunctionParametersMap;

    @JsonProperty("normalization_ratio_map")
    public Map<String, Double> normalizationRatioMap;

    @JsonProperty("score_derivation_maps")
    public Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMaps;

    @JsonProperty("target_score_derivation_inputs")
    public Map<String, String> targetScoreDerivationInputs;

    @JsonProperty("target_score_derivation_outputs")
    public Map<String, String> targetScoreDerivationOutputs;

    public enum ScoreDerivationType {
        EV, REVENUE, PROBABILITY
    }

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
