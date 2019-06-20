package com.latticeengines.domain.exposed.serviceflows.scoring.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

public class CalculateExpectedRevenuePercentileParameters extends DataFlowParameters {

    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    @JsonProperty("input_table_name")
    @SourceTableName
    private String inputTableName;

    @JsonProperty("percentile_field_name")
    private String percentileFieldName;

    @JsonProperty("original_score_field_map")
    private Map<String, String> originalScoreFieldMap;

    @JsonProperty("model_guid_field")
    private String modelGuidField;

    @JsonProperty("percentile_lower_bound")
    private Integer percentileLowerBound;

    @JsonProperty("percentile_upper_bound")
    private Integer percentileUpperBound;
    
    @JsonProperty("target_score_derivation")
    private boolean targetScoreDerivation;

    @JsonProperty("fit_function_parameters_map")
    private Map<String, String> fitFunctionParametersMap;

    @JsonProperty("normalization_ratio_map")
    private Map<String, Double> normalizationRatioMap;

    @JsonProperty("score_derivation_maps")
    private Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMaps;
    
    @JsonProperty("target_score_derivation_paths")
    private Map<String, String> targetScoreDerivationPaths;


    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getInputTableName() {
        return inputTableName;
    }

    public void setInputTableName(String inputTableName) {
        this.inputTableName = inputTableName;
    }

    public String getPercentileFieldName() {
        return percentileFieldName;
    }

    public void setPercentileFieldName(String percentileFieldName) {
        this.percentileFieldName = percentileFieldName;
    }

    public String getModelGuidField() {
        return modelGuidField;
    }

    public void setModelGuidField(String modelGuidField) {
        this.modelGuidField = modelGuidField;
    }

    public Integer getPercentileLowerBound() {
        return percentileLowerBound;
    }

    public void setPercentileLowerBound(Integer percentileLowerBound) {
        this.percentileLowerBound = percentileLowerBound;
    }

    public Integer getPercentileUpperBound() {
        return percentileUpperBound;
    }

    public void setPercentileUpperBound(Integer percentileUpperBound) {
        this.percentileUpperBound = percentileUpperBound;
    }

    public Map<String, String> getOriginalScoreFieldMap() {
        return originalScoreFieldMap;
    }

    public void setOriginalScoreFieldMap(Map<String, String> originalScoreFieldMap) {
        this.originalScoreFieldMap = originalScoreFieldMap;
    }

    public Map<String, String> getFitFunctionParametersMap() {
        return fitFunctionParametersMap;
    }

    public void setFitFunctionParametersMap(Map<String, String> fitFunctionParametersMap) {
        this.fitFunctionParametersMap = fitFunctionParametersMap;
    }

    public Map<String, Double> getNormalizationRatioMap() {
        return normalizationRatioMap;
    }

    public void setNormalizationRatioMap(Map<String, Double> normalizationRatioMap) {
        this.normalizationRatioMap = normalizationRatioMap;
    }

    public Map<String, Map<ScoreDerivationType, ScoreDerivation>> getScoreDerivationMaps() {
        return scoreDerivationMaps;
    }

    public void setScoreDerivationMaps(Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMaps) {
        this.scoreDerivationMaps = scoreDerivationMaps;
    }
    
    public boolean isTargetScoreDerivation() {
        return targetScoreDerivation;
    }

    public void setTargetScoreDerivation(boolean targetScoreDerivation) {
        this.targetScoreDerivation = targetScoreDerivation;
    }

    public Map<String, String> getTargetScoreDerivationPaths() {
        return targetScoreDerivationPaths;
    }

    public void setTargetScoreDerivationPaths(Map<String, String> targetScoreDerivationPaths) {
        this.targetScoreDerivationPaths = targetScoreDerivationPaths;
    }
    
    public enum ScoreDerivationType {
        EV, REVENUE, PROBABILITY
    }
}
