package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class DunsRedirectFromManualMatchConfig extends DunsRedirectBookConfig
        implements Serializable {
    private static final long serialVersionUID = -4746860648526830247L;

    /* Input column names */

    @NotNull
    @NotEmptyString
    @JsonProperty("ManDuns")
    private String manualSeedDuns;

    @NotNull
    @NotEmptyString
    @JsonProperty("SalesInBillions")
    private String salesInBillions;

    @NotNull
    @NotEmptyString
    @JsonProperty("TotalEmployees")
    private String totalEmployees;

    public String getManualSeedDuns() {
        return manualSeedDuns;
    }

    public void setManualSeedDuns(String manualSeedDuns) {
        this.manualSeedDuns = manualSeedDuns;
    }

    public String getSalesInBillions() {
        return salesInBillions;
    }

    public void setSalesInBillions(String salesInBillions) {
        this.salesInBillions = salesInBillions;
    }

    public String getTotalEmployees() {
        return totalEmployees;
    }

    public void setTotalEmployees(String totalEmployees) {
        this.totalEmployees = totalEmployees;
    }
}
