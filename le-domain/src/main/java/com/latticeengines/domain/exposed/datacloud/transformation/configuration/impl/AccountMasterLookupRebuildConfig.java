package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AccountMasterLookupRebuildConfig extends TransformerConfig {
    @JsonProperty("LatticeIdField")
    private String latticeIdField;

    @JsonProperty("KeyField")
    private String keyField;

    @JsonProperty("CountryField")
    private String countryField;

    @JsonProperty("StateField")
    private String stateField;

    @JsonProperty("ZipCodeField")
    private String zipCodeField;

    @JsonProperty("DomainField")
    private String domainField;

    @JsonProperty("DunsField")
    private String dunsField;

    @JsonProperty("IsPrimaryDomainField")
    private String isPrimaryDomainField;

    @JsonProperty("IsPrimaryLocationField")
    private String isPrimaryLocationField;

    @JsonProperty("GuDunsField")
    private String guDunsField;

    @JsonProperty("DuDunsField")
    private String duDunsField;

    @JsonProperty("EmployeeField")
    private String employeeField;

    @JsonProperty("SalesVolumeUsDollars")
    private String salesVolumeUsDollars;

    @JsonProperty("DomainMappingPrimaryDomainField")
    private String domainMappingPrimaryDomainField;

    @JsonProperty("DomainMappingSecondaryDomainField")
    private String domainMappingSecondaryDomainField;

    public String getLatticeIdField() {
        return latticeIdField;
    }

    public void setLatticeIdField(String latticeIdField) {
        this.latticeIdField = latticeIdField;
    }

    public String getKeyField() {
        return keyField;
    }

    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    public String getCountryField() {
        return countryField;
    }

    public void setCountryField(String countryField) {
        this.countryField = countryField;
    }

    public String getStateField() {
        return stateField;
    }

    public void setStateField(String stateField) {
        this.stateField = stateField;
    }

    public String getZipCodeField() {
        return zipCodeField;
    }

    public void setZipCodeField(String zipCodeField) {
        this.zipCodeField = zipCodeField;
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

    public String getIsPrimaryDomainField() {
        return isPrimaryDomainField;
    }

    public void setIsPrimaryDomainField(String isPrimaryDomainField) {
        this.isPrimaryDomainField = isPrimaryDomainField;
    }

    public String getIsPrimaryLocationField() {
        return isPrimaryLocationField;
    }

    public void setIsPrimaryLocationField(String isPrimaryLocationField) {
        this.isPrimaryLocationField = isPrimaryLocationField;
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

    public String getEmployeeField() {
        return employeeField;
    }

    public void setEmployeeField(String employeeField) {
        this.employeeField = employeeField;
    }

    public String getSalesVolumeUsDollars() {
        return salesVolumeUsDollars;
    }

    public void setSalesVolumeUsDollars(String salesVolumeUsDollars) {
        this.salesVolumeUsDollars = salesVolumeUsDollars;
    }

    public String getDomainMappingPrimaryDomainField() {
        return domainMappingPrimaryDomainField;
    }

    public void setDomainMappingPrimaryDomainField(String domainMappingPrimaryDomainField) {
        this.domainMappingPrimaryDomainField = domainMappingPrimaryDomainField;
    }

    public String getDomainMappingSecondaryDomainField() {
        return domainMappingSecondaryDomainField;
    }

    public void setDomainMappingSecondaryDomainField(String domainMappingSecondaryDomainField) {
        this.domainMappingSecondaryDomainField = domainMappingSecondaryDomainField;
    }

}
