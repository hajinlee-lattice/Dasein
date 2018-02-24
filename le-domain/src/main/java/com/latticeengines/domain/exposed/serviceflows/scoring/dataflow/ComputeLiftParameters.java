package com.latticeengines.domain.exposed.serviceflows.scoring.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class ComputeLiftParameters extends DataFlowParameters {

    @JsonProperty("input_table_name")
    @SourceTableName
    private String inputTableName;

    @JsonProperty("model_guid_field")
    private String modelGuidField;

    @JsonProperty("rating_field")
    private String ratingField;

    @JsonProperty("lift_field")
    private String liftField;

    @JsonProperty("score_field_map")
    private Map<String, String> scoreFieldMap;

    public String getInputTableName() {
        return inputTableName;
    }

    public void setInputTableName(String inputTableName) {
        this.inputTableName = inputTableName;
    }

    public String getModelGuidField() {
        return modelGuidField;
    }

    public void setModelGuidField(String modelGuidField) {
        this.modelGuidField = modelGuidField;
    }

    public String getRatingField() {
        return ratingField;
    }

    public void setRatingField(String ratingField) {
        this.ratingField = ratingField;
    }

    public String getLiftField() {
        return liftField;
    }

    public void setLiftField(String liftField) {
        this.liftField = liftField;
    }

    public Map<String, String> getScoreFieldMap() {
        return scoreFieldMap;
    }

    public void setScoreFieldMap(Map<String, String> scoreFieldMap) {
        this.scoreFieldMap = scoreFieldMap;
    }
}
