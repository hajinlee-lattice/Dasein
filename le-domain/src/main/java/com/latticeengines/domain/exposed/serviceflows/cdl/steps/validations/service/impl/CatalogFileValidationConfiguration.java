package com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationConfiguration;

public class CatalogFileValidationConfiguration extends InputFileValidationConfiguration {
    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    @JsonProperty("total_rows")
    private long totalRows;

    @JsonProperty("catalog_records_limit")
    private Long catalogRecordsLimit;

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

    public Long getCatalogRecordsLimit() {
        return catalogRecordsLimit;
    }

    public void setCatalogRecordsLimit(Long catalogRecordsLimit) {
        this.catalogRecordsLimit = catalogRecordsLimit;
    }
}
