package com.latticeengines.serviceflows.workflow.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.workflow.exposed.build.BaseStepConfiguration;

public class MicroserviceStepConfiguration extends BaseStepConfiguration {

    @NotEmptyString
    @NotNull
    private String customerSpace;

    @NotEmptyString
    @NotNull
    private String microServiceHostPort;

    @JsonIgnore
    public void microserviceStepConfiguration(MicroserviceStepConfiguration config) {
        this.customerSpace = config.getCustomerSpace();
        this.microServiceHostPort = config.getMicroServiceHostPort();
    }

    @JsonProperty("customerSpace")
    public String getCustomerSpace() {
        return customerSpace;
    }

    @JsonProperty("customerSpace")
    public void setCustomerSpace(String customerSpace) {
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

}
