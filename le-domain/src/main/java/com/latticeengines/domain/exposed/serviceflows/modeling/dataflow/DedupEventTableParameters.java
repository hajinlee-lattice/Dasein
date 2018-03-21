package com.latticeengines.domain.exposed.serviceflows.modeling.dataflow;

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

    @JsonProperty("event_column")
    public String eventColumn;

    public DedupEventTableParameters(String eventTable) {
        this(eventTable, DedupType.ONELEADPERDOMAIN, null);
    }

    public DedupEventTableParameters(String eventTable, DedupType deduplicationType, String eventColumn) {
        this.eventTable = eventTable;
        this.deduplicationType = deduplicationType;
        this.eventColumn = eventColumn;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public DedupEventTableParameters() {
    }
}
