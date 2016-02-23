package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class DedupEventTableParameters extends DataFlowParameters {
    @JsonProperty("event_table")
    public String eventTable;

    public DedupEventTableParameters(String eventTable) {
        this.eventTable = eventTable;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public DedupEventTableParameters() {
    }
}
