package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;

public class DedupEventTableParameters extends DataFlowParameters {
    @JsonProperty("event_table")
    @SourceTableName
    public String eventTable;

    @JsonProperty("public_domain")
    @SourceTableName
    public String publicDomain;

    @JsonProperty("deduplication_type")
    public DedupType deduplicationType;

    public DedupEventTableParameters(String eventTable, String publicDomain) {
        this(eventTable, publicDomain, DedupType.ONELEADPERDOMAIN);
    }

    public DedupEventTableParameters(String eventTable, String publicDomain, DedupType deduplicationType) {
        this.eventTable = eventTable;
        this.publicDomain = publicDomain;
        this.deduplicationType = deduplicationType;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public DedupEventTableParameters() {
    }
}
