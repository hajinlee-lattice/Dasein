package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class PrepareDunsRedirectManualMatchConfig extends TransformerConfig
        implements Serializable {
    public static final String LARGE_COMPANY_EMPLOYEE_SIZE = "Large";
    public static final String SMALL_COMPANY_EMPLOYEE_SIZE = "Small";

    private static final long serialVersionUID = 41269480844374L;

    /* Manual seed column names */

    @NotNull
    @NotEmptyString
    @JsonProperty("ManId")
    private String manId;

    @NotNull
    @NotEmptyString
    @JsonProperty("ManDUNS")
    private String duns;

    @NotNull
    @NotEmptyString
    @JsonProperty("ManCompanyName")
    private String companyName;

    @NotNull
    @NotEmptyString
    @JsonProperty("ManCountry")
    private String country;

    @NotNull
    @NotEmptyString
    @JsonProperty("ManState")
    private String state;

    @NotNull
    @NotEmptyString
    @JsonProperty("ManCity")
    private String city;

    @NotNull
    @NotEmptyString
    @JsonProperty("ManSalesInBillions")
    private String salesInBillions;

    @NotNull
    @NotEmptyString
    @JsonProperty("ManTotalEmployees")
    private String totalEmployees;

    @NotNull
    @NotEmptyString
    @JsonProperty("ManEmpSize")
    private String employeeSize;

    public String getManId() {
        return manId;
    }

    public void setManId(String manId) {
        this.manId = manId;
    }

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
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

    public String getEmployeeSize() {
        return employeeSize;
    }

    public void setEmployeeSize(String employeeSize) {
        this.employeeSize = employeeSize;
    }
}
