package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class AddStandardAttributesParameters extends DataFlowParameters {
    @JsonProperty("event_table")
    public String eventTable;

    public AddStandardAttributesParameters(String eventTable) {
        this.eventTable = eventTable;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public AddStandardAttributesParameters() {
    }
}
