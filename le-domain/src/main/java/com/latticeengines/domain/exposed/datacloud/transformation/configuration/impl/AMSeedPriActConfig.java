package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AMSeedPriActConfig extends TransformerConfig {
    @JsonProperty("LatticeIdField")
    private String latticeIdField;

    @JsonProperty("CountryField")
    private String countryField;

    @JsonProperty("DomainField")
    private String domainField;

    @JsonProperty("DunsField")
    private String dunsField;

    @JsonProperty("GuDunsField")
    private String guDunsField;

    @JsonProperty("DuDunsField")
    private String duDunsField;

    @JsonProperty("IsPriLocField")
    private String isPriLocField;

    @JsonProperty("IsPriActField")
    private String isPriActField;

    @JsonProperty("EmployeeField")
    private String employeeField;

    @JsonProperty("SalesVolUSField")
    private String salesVolUSField;

    public String getLatticeIdField() {
        return latticeIdField;
    }

    public void setLatticeIdField(String latticeIdField) {
        this.latticeIdField = latticeIdField;
    }

    public String getCountryField() {
        return countryField;
    }

    public void setCountryField(String countryField) {
        this.countryField = countryField;
    }

    public String getDomainField() {
        return domainField;
    }

    public void setDomainField(String domainField) {
        this.domainField = domainField;
    }

    public String getDunsField() {
        return dunsField;
    }

    public void setDunsField(String dunsField) {
        this.dunsField = dunsField;
    }

    public String getGuDunsField() {
        return guDunsField;
    }

    public void setGuDunsField(String guDunsField) {
        this.guDunsField = guDunsField;
    }

    public String getDuDunsField() {
        return duDunsField;
    }

    public void setDuDunsField(String duDunsField) {
        this.duDunsField = duDunsField;
    }

    public String getIsPriLocField() {
        return isPriLocField;
    }

    public void setIsPriLocField(String isPriLocField) {
        this.isPriLocField = isPriLocField;
    }

    public String getIsPriActField() {
        return isPriActField;
    }

    public void setIsPriActField(String isPriActField) {
        this.isPriActField = isPriActField;
    }

    public String getEmployeeField() {
        return employeeField;
    }

    public void setEmployeeField(String employeeField) {
        this.employeeField = employeeField;
    }

    public String getSalesVolUSField() {
        return salesVolUSField;
    }

    public void setSalesVolUSField(String salesVolUSField) {
        this.salesVolUSField = salesVolUSField;
    }

}
