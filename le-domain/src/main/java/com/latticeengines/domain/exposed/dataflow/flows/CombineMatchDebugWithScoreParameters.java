package com.latticeengines.domain.exposed.dataflow.flows;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class CombineMatchDebugWithScoreParameters extends DataFlowParameters {
    @JsonProperty("score_results_table_name")
    @SourceTableName
    private String scoreResultsTableName;

    @JsonProperty("input_table_name")
    @SourceTableName
    private String inputTableName;

    @JsonProperty("columns_to_retain")
    private List<String> columnsToRetain;

    public CombineMatchDebugWithScoreParameters(String scoreResultsTable, String postMatchTable) {
        setScoreResultsTableName(scoreResultsTable);
        setInputTableName(postMatchTable);
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public CombineMatchDebugWithScoreParameters() {
    }

    public String getScoreResultsTableName() {
        return scoreResultsTableName;
    }

    public void setScoreResultsTableName(String scoreResultsTableName) {
        this.scoreResultsTableName = scoreResultsTableName;
    }

    public String getInputTableName() {
        return inputTableName;
    }

    public void setInputTableName(String inputTableName) {
        this.inputTableName = inputTableName;
    }

    public List<String> getColumnsToRetain() {
        return columnsToRetain;
    }

    public void setColumnsToRetain(List<String> columnsToRetain) {
        this.columnsToRetain = columnsToRetain;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
