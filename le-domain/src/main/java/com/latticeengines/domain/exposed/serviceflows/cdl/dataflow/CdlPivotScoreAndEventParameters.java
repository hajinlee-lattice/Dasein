package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class CdlPivotScoreAndEventParameters extends DataFlowParameters {

    @JsonProperty("score_output_table_name")
    @SourceTableName
    @NotEmptyString
    @NotNull
    private String scoreOutputTableName;

    @JsonProperty("avg_scores")
    private Map<String, Double> avgScores;

    @JsonProperty("is_expected_value")
    private Map<String, Boolean> expectedValue;

    public CdlPivotScoreAndEventParameters() {
    }

    public CdlPivotScoreAndEventParameters(String scoreOutputTableName) {
        setScoreOutputTableName(scoreOutputTableName);
    }

    public String getScoreOutputTableName() {
        return scoreOutputTableName;
    }

    public void setScoreOutputTableName(String scoreOutputTableName) {
        this.scoreOutputTableName = scoreOutputTableName;
    }

    public Map<String, Double> getAvgScores() {
        return avgScores;
    }

    public void setAvgScores(Map<String, Double> avgScores) {
        this.avgScores = avgScores;
    }

    public Map<String, Boolean> isExpectedValue() {
        return expectedValue;
    }

    public void setExpectedValue(Map<String, Boolean> expectedValue) {
        this.expectedValue = expectedValue;
    }

}
