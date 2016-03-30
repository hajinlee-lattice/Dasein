package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class CombineInputTableWithScoreParameters extends DataFlowParameters {
    @JsonProperty("score_results_table_name")
    private String scoreResultsTableName;

    @JsonProperty("input_table_name")
    private String inputTableName;

    private boolean debug = false;

    public CombineInputTableWithScoreParameters(String scoreResultsTable, String trainingTable) {
        setScoreResultsTableName(scoreResultsTable);
        setInputTableName(trainingTable);
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public CombineInputTableWithScoreParameters() {
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

    public boolean isDebuggingEnabled() {
        return debug;
    }

    public void enableDebugging() {
        this.debug = true;
    }

}
