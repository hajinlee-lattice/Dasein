package com.latticeengines.domain.exposed.serviceflows.scoring.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class PivotScoreAndEventParameters extends DataFlowParameters {

    @JsonProperty("score_output_table_name")
    @SourceTableName
    @NotEmptyString
    @NotNull
    private String scoreOutputTableName;

    @JsonProperty("event_column")
    private String eventColumn;

    @JsonProperty("avg_scores")
    private Map<String, Double> avgScores;

    @JsonProperty("is_expected_value")
    private Map<String, Boolean> expectedValue;

    /**
     * Serialization constructor
     */
    @Deprecated
    public PivotScoreAndEventParameters() {
    }

    public PivotScoreAndEventParameters(String scoreOutputTableName) {
        setScoreOutputTableName(scoreOutputTableName);
    }

    public String getScoreOutputTableName() {
        return scoreOutputTableName;
    }

    public void setScoreOutputTableName(String scoreOutputTableName) {
        this.scoreOutputTableName = scoreOutputTableName;
    }

    public String getEventColumn() {
        return eventColumn;
    }

    public void setEventColumn(String eventColumn) {
        this.eventColumn = eventColumn;
    }

    public Map<String, Double> getAvgScores() {
        return avgScores;
    }

    public void setAvgScores(Map<String, Double> avgScores) {
        this.avgScores = avgScores;
    }

    public Map<String, Boolean> getExpectedValues() {
        return expectedValue;
    }

    public void setExpectedValues(Map<String, Boolean> expectedValue) {
        this.expectedValue = expectedValue;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
