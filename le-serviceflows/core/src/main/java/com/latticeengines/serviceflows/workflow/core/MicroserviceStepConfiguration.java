package com.latticeengines.serviceflows.workflow.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class MicroserviceStepConfiguration extends BaseStepConfiguration {
    
    private String podId;

    @NotNull
    private CustomerSpace customerSpace;

    @NotEmptyString
    @NotNull
    private String microServiceHostPort;

    @JsonIgnore
    public void microserviceStepConfiguration(MicroserviceStepConfiguration config) {
        this.customerSpace = config.getCustomerSpace();
        this.microServiceHostPort = config.getMicroServiceHostPort();
    }

    @JsonProperty("customerSpace")
    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    @JsonProperty("customerSpace")
    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    @JsonProperty("microServiceHostPort")
    public String getMicroServiceHostPort() {
        return microServiceHostPort;
    }

    @JsonProperty("microServiceHostPort")
    public void setMicroServiceHostPort(String microServiceHostPort) {
        this.microServiceHostPort = microServiceHostPort;
    }

    @JsonProperty("podId")
    public String getPodId() {
        return podId;
    }

    @JsonProperty("podId")
    public void setPodId(String podId) {
        this.podId = podId;
    }

}
