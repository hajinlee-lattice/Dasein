package com.latticeengines.domain.exposed.serviceflows.scoring.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

public class PivotScoreAndEventParameters extends DataFlowParameters {

    @JsonProperty("score_output_table_name")
    @SourceTableName
    @NotEmptyString
    @NotNull
    private String scoreOutputTableName;

    @JsonProperty("event_column")
    private String eventColumn;

    @JsonProperty("avg_scores")
    private Map<String, Double> avgScores = new HashMap<>();

    @JsonProperty("score_field_map")
    private Map<String, String> scoreFieldMap = new HashMap<>();

    @JsonProperty("score_derivation_map")
    private Map<String, String> scoreDerivationMap = new HashMap<>();

    @JsonProperty("fit_function_parameters_map")
    private Map<String, String> fitFunctionParametersMap = new HashMap<>();

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

    public Map<String, String> getScoreFieldMap() {
        return scoreFieldMap;
    }

    public void setScoreFieldMap(Map<String, String> scoreFieldMap) {
        this.scoreFieldMap = scoreFieldMap;
    }

    public Map<String, String> getScoreDerivationMap() {
        return scoreDerivationMap;
    }

    public void setScoreDerivationMap(Map<String, String> scoreDerivationMap) {
        this.scoreDerivationMap = scoreDerivationMap;
    }

    public Map<String, String> getFitFunctionParametersMap() {
        return fitFunctionParametersMap;
    }

    public void setFitFunctionParametersMap(Map<String, String> fitFunctionParametersMap) {
        this.fitFunctionParametersMap = fitFunctionParametersMap;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
