package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class CreateScoreTableParameters extends DataFlowParameters {
    @JsonProperty("score_results_table")
    private String scoreResultsTable;

    @JsonProperty("event_table")
    private String eventTable;
    
    @JsonProperty("uk_column")
    private String uniqueKeyColumn;

    public CreateScoreTableParameters(String scoreResultsTable, String eventTable, String uniqueKeyColumn) {
        setScoreResultsTable(scoreResultsTable);
        setEventTable(eventTable);
        setUniqueKeyColumn(uniqueKeyColumn);
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public CreateScoreTableParameters() {
    }

    public String getScoreResultsTable() {
        return scoreResultsTable;
    }

    public void setScoreResultsTable(String scoreResultsTable) {
        this.scoreResultsTable = scoreResultsTable;
    }

    public String getEventTable() {
        return eventTable;
    }

    public void setEventTable(String eventTable) {
        this.eventTable = eventTable;
    }

    public String getUniqueKeyColumn() {
        return uniqueKeyColumn;
    }

    public void setUniqueKeyColumn(String uniqueKeyColumn) {
        this.uniqueKeyColumn = uniqueKeyColumn;
    }

}
