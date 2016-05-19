package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class DedupEventTableParameters extends DataFlowParameters {
    @JsonProperty("event_table")
    public String eventTable;

    @JsonProperty("public_domain")
    public String publicDomain;

    public DedupEventTableParameters(String eventTable, String publicDomain) {
        this.eventTable = eventTable;
        this.publicDomain = publicDomain;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public DedupEventTableParameters() {
    }
}
