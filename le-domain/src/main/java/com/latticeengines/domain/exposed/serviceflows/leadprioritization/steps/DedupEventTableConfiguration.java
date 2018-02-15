package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class DedupEventTableConfiguration extends BaseLPDataFlowStepConfiguration {

    @JsonProperty("dedup_type")
    private DedupType dedupType;

    public DedupType getDedupType() {
        return dedupType;
    }

    public void setDedupType(DedupType dedupType) {
        this.dedupType = dedupType;
    }

    @Override
    public String getSwlib() {
        return SoftwareLibrary.Modeling.getName();
    }
}
