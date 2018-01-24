package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class ScoreAggregateParameters extends DataFlowParameters {
    @JsonProperty("score_results_table_name")
    @SourceTableName
    private String scoreResultsTableName;

    public String getScoreResultsTableName() {
        return scoreResultsTableName;
    }

    public void setScoreResultsTableName(String scoreResultsTableName) {
        this.scoreResultsTableName = scoreResultsTableName;
    }

}
