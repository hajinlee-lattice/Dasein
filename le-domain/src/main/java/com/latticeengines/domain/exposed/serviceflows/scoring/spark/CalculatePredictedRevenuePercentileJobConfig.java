package com.latticeengines.domain.exposed.serviceflows.scoring.spark;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig.ScoreDerivationType;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CalculatePredictedRevenuePercentileJobConfig extends SparkJobConfig {

    private static final long serialVersionUID = -1533497653914800071L;

    public static final String NAME = "calculatePredictedRevenuePercentileJobFlow";

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

    @JsonProperty("score_derivation_maps")
    public Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMaps;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
