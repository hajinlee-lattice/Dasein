package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class DedupEventTableConfiguration extends DataFlowStepConfiguration {

    @JsonProperty("dedup_type")
    private DedupType dedupType;

    public DedupType getDedupType() {
        return dedupType;
    }

    public void setDedupType(DedupType dedupType) {
        this.dedupType = dedupType;
    }
}
