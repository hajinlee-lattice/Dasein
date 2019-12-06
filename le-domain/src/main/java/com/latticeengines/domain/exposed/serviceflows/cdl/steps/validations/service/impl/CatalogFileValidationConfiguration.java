package com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationConfiguration;

public class CatalogFileValidationConfiguration extends InputFileValidationConfiguration {
    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    @JsonProperty("total_rows")
    private long totalRows;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(long totalRows) {
        this.totalRows = totalRows;
    }
}
