package com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;

public class DedupEventTableParameters extends DataFlowParameters {
    @JsonProperty("event_table")
    @SourceTableName
    public String eventTable;

    @JsonProperty("deduplication_type")
    public DedupType deduplicationType;

    public DedupEventTableParameters(String eventTable) {
        this(eventTable, DedupType.ONELEADPERDOMAIN);
    }

    public DedupEventTableParameters(String eventTable, DedupType deduplicationType) {
        this.eventTable = eventTable;
        this.deduplicationType = deduplicationType;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public DedupEventTableParameters() {
    }
}
