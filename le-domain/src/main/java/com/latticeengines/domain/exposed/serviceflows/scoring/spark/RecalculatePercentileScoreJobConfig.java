package com.latticeengines.domain.exposed.serviceflows.scoring.spark;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class RecalculatePercentileScoreJobConfig extends SparkJobConfig {

    private static final long serialVersionUID = 6761873334869605574L;

    public static final String NAME = "recalculatePercentileScoreJobJobFlow";

    @JsonProperty("input_table_name")
    @SourceTableName
    public String inputTableName;

    @JsonProperty("raw_score_field_name")
    public String rawScoreFieldName;

    @JsonProperty("score_field_name")
    public String scoreFieldName;

    @JsonProperty("model_guid_field")
    public String modelGuidField;

    @JsonProperty("percentile_lower_bound")
    public Integer percentileLowerBound;

    @JsonProperty("percentile_upper_bound")
    public Integer percentileUpperBound;

    @JsonProperty("target_score_derivation")
    public boolean targetScoreDerivation;

    @JsonProperty("original_score_field_map")
    public Map<String, String> originalScoreFieldMap;

    @JsonProperty("target_score_derivation_paths")
    public Map<String, String> targetScoreDerivationPaths;

    @JsonProperty("target_score_derivation_inputs")
    public Map<String, String> targetScoreDerivationInputs;

    @JsonProperty("target_score_derivation_outputs")
    public Map<String, String> targetScoreDerivationOutputs;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
