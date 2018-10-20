package com.latticeengines.domain.exposed.serviceflows.scoring.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class RecalculatePercentileScoreParameters extends DataFlowParameters {
    @JsonProperty("input_table_name")
    @SourceTableName
    private String inputTableName;

    @JsonProperty("raw_score_field_name")
    private String rawScoreFieldName;

    @JsonProperty("score_field_name")
    private String scoreFieldName;

    @JsonProperty("model_guid_field")
    private String modelGuidField;

    @JsonProperty("percentile_lower_bound")
    private Integer percentileLowerBound;

    @JsonProperty("percentile_upper_bound")
    private Integer percentileUpperBound;

    public String getInputTableName() {
        return inputTableName;
    }

    public void setInputTableName(String inputTableName) {
        this.inputTableName = inputTableName;
    }

    public String getRawScoreFieldName() {
        return rawScoreFieldName;
    }

    public void setRawScoreFieldName(String rawScoreFieldName) {
        this.rawScoreFieldName = rawScoreFieldName;
    }

    public String getScoreFieldName() {
        return scoreFieldName;
    }

    public void setScoreFieldName(String scoreFieldName) {
        this.scoreFieldName = scoreFieldName;
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
}
