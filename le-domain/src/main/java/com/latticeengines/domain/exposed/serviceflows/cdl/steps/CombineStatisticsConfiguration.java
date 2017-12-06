package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class CombineStatisticsConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer_space")
    @NotNull
    private CustomerSpace customerSpace;

    public CustomerSpace getCustomerSpace() {
        return this.customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }
}
