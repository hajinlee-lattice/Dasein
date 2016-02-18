package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class DedupEventTableParameters extends DataFlowParameters {
    @JsonProperty("event_table")
    public String eventTable;

    @JsonProperty("website_column")
    public String websiteColumn;

    @JsonProperty("email_column")
    public String emailColumn;

    @JsonProperty("event_column")
    public String eventColumn;

    public DedupEventTableParameters(String eventTable, String websiteColumn, String emailColumn, String eventColumn) {
        this.eventTable = eventTable;
        this.websiteColumn = websiteColumn;
        this.eventColumn = eventColumn;
        this.emailColumn = emailColumn;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public DedupEventTableParameters() {
    }
}
