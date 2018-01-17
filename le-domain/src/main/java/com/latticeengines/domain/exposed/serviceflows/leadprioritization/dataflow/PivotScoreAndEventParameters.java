package com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow;

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

    @JsonProperty("model_avg_probability")
    private double modelAvgProbability;

    @JsonProperty("event_column")
    private String eventColumn;

    /**
     * Serialization constructor
     */
    @Deprecated
    public PivotScoreAndEventParameters() {
    }

    public PivotScoreAndEventParameters(String scoreOutputTableName, double modelAvgProbability) {
        setScoreOutputTableName(scoreOutputTableName);
        setModelAvgProbability(modelAvgProbability);
    }

    public String getScoreOutputTableName() {
        return scoreOutputTableName;
    }

    public void setScoreOutputTableName(String scoreOutputTableName) {
        this.scoreOutputTableName = scoreOutputTableName;
    }

    public double getModelAvgProbability() {
        return modelAvgProbability;
    }

    public void setModelAvgProbability(double modelAvgProbability) {
        this.modelAvgProbability = modelAvgProbability;
    }

    public String getEventColumn() {
        return eventColumn;
    }

    public void setEventColumn(String eventColumn) {
        this.eventColumn = eventColumn;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
