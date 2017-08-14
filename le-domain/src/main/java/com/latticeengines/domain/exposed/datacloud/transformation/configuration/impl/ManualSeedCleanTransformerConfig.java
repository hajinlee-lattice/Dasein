package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ManualSeedCleanTransformerConfig extends TransformerConfig {

    @JsonProperty("SALES_VOLUME_US_DOLLARS")
    private String salesVolumeInUSDollars;

    @JsonProperty("EMPLOYEES_TOTAL")
    private String employeesTotal;

    @JsonProperty("ManSeedDomain")
    private String manSeedDomain;

    @JsonProperty("ManSeedDuns")
    private String manSeedDuns;

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

    public String getManSeedDomain() {
        return manSeedDomain;
    }

    public void setManSeedDomain(String manSeedDomain) {
        this.manSeedDomain = manSeedDomain;
    }

    public String getManSeedDuns() {
        return manSeedDuns;
    }

    public void setManSeedDuns(String manSeedDuns) {
        this.manSeedDuns = manSeedDuns;
    }

}
