package com.latticeengines.domain.exposed.dataflow.flows;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class CreateAttributeLevelSummaryParameters extends DataFlowParameters {

    @JsonProperty("event_table")
    public String eventTable;
    
    @JsonProperty("groupby_columns")
    public List<String> groupByColumns;
    
    @JsonProperty("aggregate_column")
    public String aggregateColumn;
    
    @JsonProperty("aggregation_type")
    public String aggregationType;
    
    public CreateAttributeLevelSummaryParameters(String eventTable, List<String> groupByColumns, String aggregateColumn) {
        this.eventTable = eventTable;
        this.groupByColumns = groupByColumns;
        this.aggregateColumn = aggregateColumn;
        this.aggregationType = "COUNT";
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public CreateAttributeLevelSummaryParameters() {
    }

}
