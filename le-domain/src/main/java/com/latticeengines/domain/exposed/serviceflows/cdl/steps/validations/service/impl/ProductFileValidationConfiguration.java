package com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationConfiguration;


public class ProductFileValidationConfiguration extends InputFileValidationConfiguration {

    @JsonProperty("customerSpace")
    private CustomerSpace customerSpace;

    @JsonProperty("dataFeedTaskId")
    private String dataFeedTaskId;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getDataFeedTaskId() {
        return dataFeedTaskId;
    }

    public void setDataFeedTaskId(String dataFeedTaskId) {
        this.dataFeedTaskId = dataFeedTaskId;
    }
}
