package com.latticeengines.domain.exposed.serviceflows.scoring.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class CalculatePredictedRevenuePercentileParameters extends DataFlowParameters {

    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    @JsonProperty("input_table_name")
    @SourceTableName
    private String inputTableName;

    @JsonProperty("percentile_field_name")
    private String percentileFieldName;

    @JsonProperty("revenue_field_name")
    private String revenueFieldName;

    @JsonProperty("original_score_field_map")
    private Map<String, String> originalScoreFieldMap;

    @JsonProperty("model_guid_field")
    private String modelGuidField;

    @JsonProperty("percentile_lower_bound")
    private Integer percentileLowerBound;

    @JsonProperty("percentile_upper_bound")
    private Integer percentileUpperBound;
    
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

    public String getRevenueFieldName() {
        return revenueFieldName;
    }

    public void setRevenueFieldName(String revenueFieldName) {
        this.revenueFieldName = revenueFieldName;
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
}
