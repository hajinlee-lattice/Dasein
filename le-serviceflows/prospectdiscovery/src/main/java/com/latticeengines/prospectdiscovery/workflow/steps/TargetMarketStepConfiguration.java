package com.latticeengines.prospectdiscovery.workflow.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class TargetMarketStepConfiguration extends MicroserviceStepConfiguration {
    @NotNull
    @JsonProperty("target_market")
    private TargetMarket targetMarket;

    @NotEmptyString
    @NotNull
    private String internalResourceHostPort;

    public TargetMarket getTargetMarket() {
        return targetMarket;
    }

    public void setTargetMarket(TargetMarket targetMarket) {
        this.targetMarket = targetMarket;
    }
    
    public String getInternalResourceHostPort() {
        return internalResourceHostPort;
    }

    public void setInternalResourceHostPort(String internalResourceHostPort) {
        this.internalResourceHostPort = internalResourceHostPort;
    }

    
}
