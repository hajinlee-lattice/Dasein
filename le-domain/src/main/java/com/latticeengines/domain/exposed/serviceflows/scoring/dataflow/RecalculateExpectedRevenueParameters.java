package com.latticeengines.domain.exposed.serviceflows.scoring.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class RecalculateExpectedRevenueParameters extends DataFlowParameters {
    @JsonProperty("input_table_name")
    @SourceTableName
    private String inputTableName;

    @JsonProperty("expected_revenue_field_name")
    private String expectedRevenueFieldName;

    @JsonProperty("percentile_field_name")
    private String percentileFieldName;

    @JsonProperty("predicted_revenue_percentile_field_name")
    private String predictedRevenuePercentileFieldName;

    @JsonProperty("original_score_field_map")
    private Map<String, String> originalScoreFieldMap;

    @JsonProperty("model_guid_field")
    private String modelGuidField;

    @JsonProperty("fit_function_parameters_map")
    private Map<String, String> fitFunctionParametersMap = new HashMap<>();

    public String getInputTableName() {
        return inputTableName;
    }

    public void setInputTableName(String inputTableName) {
        this.inputTableName = inputTableName;
    }

    public String getExpectedRevenueFieldName() {
        return expectedRevenueFieldName;
    }

    public void setExpectedRevenueFieldName(String expectedRevenueFieldName) {
        this.expectedRevenueFieldName = expectedRevenueFieldName;
    }

    public String getPercentileFieldName() {
        return percentileFieldName;
    }

    public void setPercentileFieldName(String percentileFieldName) {
        this.percentileFieldName = percentileFieldName;
    }

    public String getPredictedRevenuePercentileFieldName() {
        return predictedRevenuePercentileFieldName;
    }

    public void setPredictedRevenuePercentileFieldName(String predictedRevenuePercentileFieldName) {
        this.predictedRevenuePercentileFieldName = predictedRevenuePercentileFieldName;
    }

    public Map<String, String> getOriginalScoreFieldMap() {
        return originalScoreFieldMap;
    }

    public void setOriginalScoreFieldMap(Map<String, String> originalScoreFieldMap) {
        this.originalScoreFieldMap = originalScoreFieldMap;
    }

    public String getModelGuidField() {
        return modelGuidField;
    }

    public void setModelGuidField(String modelGuidField) {
        this.modelGuidField = modelGuidField;
    }

    public Map<String, String> getFitFunctionParametersMap() {
        return fitFunctionParametersMap;
    }

    public void setFitFunctionParametersMap(Map<String, String> fitFunctionParametersMap) {
        this.fitFunctionParametersMap = fitFunctionParametersMap;
    }
}
