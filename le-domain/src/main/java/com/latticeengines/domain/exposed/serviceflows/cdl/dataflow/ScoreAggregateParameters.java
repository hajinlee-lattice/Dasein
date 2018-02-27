package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class ScoreAggregateParameters extends DataFlowParameters {

    @JsonProperty("score_results_table_name")
    @SourceTableName
    private String scoreResultsTableName;

    @JsonProperty("model_guid_field")
    private String modelGuidField;

    @JsonProperty("score_field_map")
    private Map<String, String> scoreFieldMap;

    @JsonProperty("expected_value")
    private Boolean expectedValue;

    public String getScoreResultsTableName() {
        return scoreResultsTableName;
    }

    public void setScoreResultsTableName(String scoreResultsTableName) {
        this.scoreResultsTableName = scoreResultsTableName;
    }

    public String getModelGuidField() {
        return modelGuidField;
    }

    public void setModelGuidField(String modelGuidField) {
        this.modelGuidField = modelGuidField;
    }

    public Map<String, String> getScoreFieldMap() {
        return scoreFieldMap;
    }

    public void setScoreFieldMap(Map<String, String> scoreFieldMap) {
        this.scoreFieldMap = scoreFieldMap;
    }

    public Boolean getExpectedValue() {
        return expectedValue;
    }

    public void setExpectedValue(Boolean expectedValue) {
        this.expectedValue = expectedValue;
    }
}
