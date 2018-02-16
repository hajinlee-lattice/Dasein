package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

import java.util.Map;

public class ScoreAggregateParameters extends DataFlowParameters {

    @JsonProperty("score_results_table_name")
    @SourceTableName
    private String scoreResultsTableName;

    @JsonProperty("model_guid_field")
    private String modelGuidField;

    @JsonProperty("score_field_map")
    private Map<String, String> scoreFieldMap;

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
}
