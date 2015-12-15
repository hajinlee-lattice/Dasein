package com.latticeengines.prospectdiscovery.workflow.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class TargetMarketStepConfiguration extends MicroserviceStepConfiguration {
    @NotNull
    @JsonProperty("target_market")
    private TargetMarket targetMarket;

    public TargetMarket getTargetMarket() {
        return targetMarket;
    }

    public void setTargetMarket(TargetMarket targetMarket) {
        this.targetMarket = targetMarket;
    }
}
