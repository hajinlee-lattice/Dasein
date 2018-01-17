package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class CreateCdlEventTableFilterParameters extends DataFlowParameters {

    @JsonProperty("train_filter_table")
    @SourceTableName
    public String trainFilterTable;

    @JsonProperty("event_filter_table")
    @SourceTableName
    public String eventFilterTable;

    private String eventColumn;

    public CreateCdlEventTableFilterParameters(String trainFilterTable, String eventFilterTable) {
        this.trainFilterTable = trainFilterTable;
        this.eventFilterTable = eventFilterTable;
    }

    public CreateCdlEventTableFilterParameters() {
    }

    public String getEventColumn() {
        return eventColumn;
    }

    public void setEventColumn(String eventColumn) {
        this.eventColumn = eventColumn;
    }

}
