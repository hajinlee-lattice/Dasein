package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

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

    @JsonProperty("avg_score")
    private Double avgScore;

    @JsonProperty("is_expected_value")
    private boolean expectedValue;

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

    public Double getAvgScore() {
        return avgScore;
    }

    public void setAvgScore(Double avgScore) {
        this.avgScore = avgScore;
    }

    public boolean isExpectedValue() {
        return expectedValue;
    }

    public void setExpectedValue(boolean expectedValue) {
        this.expectedValue = expectedValue;
    }

}
