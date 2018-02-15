package com.latticeengines.domain.exposed.serviceflows.modeling.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;

public class DedupEventTableConfiguration extends BaseModelingDataFlowStepConfiguration {

    @JsonProperty("dedup_type")
    private DedupType dedupType;

    public DedupType getDedupType() {
        return dedupType;
    }

    public void setDedupType(DedupType dedupType) {
        this.dedupType = dedupType;
    }

}
