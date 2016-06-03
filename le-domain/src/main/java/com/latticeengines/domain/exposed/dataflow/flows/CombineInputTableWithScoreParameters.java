package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class CombineInputTableWithScoreParameters extends DataFlowParameters {
    @JsonProperty("score_results_table_name")
    @SourceTableName
    private String scoreResultsTableName;

    @JsonProperty("input_table_name")
    @SourceTableName
    private String inputTableName;

    @JsonProperty("debug_enabled")
    private boolean debuggingEnabled = false;

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

    public void setDebuggingEnabled(boolean debuggingEnabled) {
        this.debuggingEnabled = debuggingEnabled;
    }

    public boolean isDebuggingEnabled() {
        return debuggingEnabled;
    }

    public void enableDebugging() {
        this.debuggingEnabled = true;
    }


    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
