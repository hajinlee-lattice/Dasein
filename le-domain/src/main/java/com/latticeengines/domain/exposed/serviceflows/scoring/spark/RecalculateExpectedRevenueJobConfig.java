package com.latticeengines.domain.exposed.serviceflows.scoring.spark;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class RecalculateExpectedRevenueJobConfig extends SparkJobConfig {

    private static final long serialVersionUID = -7855267639841928430L;

    public static final String NAME = "recalculateExpectedRevenueJobFlow";
    
    @JsonProperty("input_table_name")
    public String inputTableName;

    @JsonProperty("expected_revenue_field_name")
    public String expectedRevenueFieldName;

    @JsonProperty("percentile_field_name")
    public String percentileFieldName;

    @JsonProperty("predicted_revenue_percentile_field_name")
    public String predictedRevenuePercentileFieldName;

    @JsonProperty("original_score_field_map")
    public Map<String, String> originalScoreFieldMap;

    @JsonProperty("model_guid_field")
    public String modelGuidField;

    @JsonProperty("fit_function_parameters_map")
    public Map<String, String> fitFunctionParametersMap = new HashMap<>();
    
    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
