package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DnBCleanConfig extends TransformerConfig {
    @JsonProperty("SalesVolumeUSField")
    private String salesVolumeUSField;

    @JsonProperty("SalesVolumeCodeField")
    private String salesVolumeCodeField;

    @JsonProperty("EmployeeTotalField")
    private String employeeTotalField;

    @JsonProperty("EmployeeTotalCodeField")
    private String employeeTotalCodeField;

    @JsonProperty("EmployeeHereField")
    private String employeeHereField;

    @JsonProperty("EmployeeHereCodeField")
    private String employeeHereCodeField;

    public String getSalesVolumeUSField() {
        return salesVolumeUSField;
    }

    public void setSalesVolumeUSField(String salesVolumeUSField) {
        this.salesVolumeUSField = salesVolumeUSField;
    }

    public String getSalesVolumeCodeField() {
        return salesVolumeCodeField;
    }

    public void setSalesVolumeCodeField(String salesVolumeCodeField) {
        this.salesVolumeCodeField = salesVolumeCodeField;
    }

    public String getEmployeeTotalField() {
        return employeeTotalField;
    }

    public void setEmployeeTotalField(String employeeTotalField) {
        this.employeeTotalField = employeeTotalField;
    }

    public String getEmployeeTotalCodeField() {
        return employeeTotalCodeField;
    }

    public void setEmployeeTotalCodeField(String employeeTotalCodeField) {
        this.employeeTotalCodeField = employeeTotalCodeField;
    }

    public String getEmployeeHereField() {
        return employeeHereField;
    }

    public void setEmployeeHereField(String employeeHereField) {
        this.employeeHereField = employeeHereField;
    }

    public String getEmployeeHereCodeField() {
        return employeeHereCodeField;
    }

    public void setEmployeeHereCodeField(String employeeHereCodeField) {
        this.employeeHereCodeField = employeeHereCodeField;
    }


}
