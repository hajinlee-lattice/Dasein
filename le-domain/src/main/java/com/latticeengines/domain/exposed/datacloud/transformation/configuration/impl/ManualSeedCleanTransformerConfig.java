package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ManualSeedCleanTransformerConfig extends TransformerConfig {

    @JsonProperty("SALES_VOLUME_US_DOLLARS")
    private String salesVolumeInUSDollars;

    @JsonProperty("EMPLOYEES_TOTAL")
    private String employeesTotal;

    public String getSalesVolumeInUSDollars() {
        return salesVolumeInUSDollars;
    }

    public void setSalesVolumeInUSDollars(String salesVolumeInUSDollars) {
        this.salesVolumeInUSDollars = salesVolumeInUSDollars;
    }

    public String getEmployeesTotal() {
        return employeesTotal;
    }

    public void setEmployeesTotal(String employeesTotal) {
        this.employeesTotal = employeesTotal;
    }

}
