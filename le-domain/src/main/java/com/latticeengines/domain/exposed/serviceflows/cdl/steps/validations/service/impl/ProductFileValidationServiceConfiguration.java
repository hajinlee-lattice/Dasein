package com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationServiceConfiguration;


public class ProductFileValidationServiceConfiguration extends InputFileValidationServiceConfiguration {
    private CustomerSpace customerSpace;

    @JsonProperty("customerSpace")
    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    @JsonProperty("customerSpace")
    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

}
