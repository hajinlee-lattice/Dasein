package com.latticeengines.domain.exposed.workflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class BaseWrapperStepConfiguration extends BaseStepConfiguration {

    @NotNull
    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    @JsonProperty("pod_id")
    private String podId = "";

    @JsonProperty("phase")
    private Phase phase = Phase.PRE_PROCESSING;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getPodId() {
        return podId;
    }

    public void setPodId(String podId) {
        this.podId = podId;
    }

    public Phase getPhase() {
        return phase;
    }

    public void setPhase(Phase phase) {
        this.phase = phase;
    }

    public enum Phase {
        PRE_PROCESSING, POST_PROCESSING
    }

}
