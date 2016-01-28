package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LeadEnrichmentAttribute {

    private String displayName;
    private String fieldName;
    private String fieldType;
    private String customerColumnName;
    private String dataSource;
    private String description;

    @JsonProperty("DisplayName")
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("DisplayName")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonProperty("FieldName")
    public String getFieldName() {
        return fieldName;
    }

    @JsonProperty("FieldName")
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    @JsonProperty("FieldType")
    public String getFieldType() {
        return fieldType;
    }

    @JsonProperty("FieldType")
    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    @JsonProperty("CustomerColumnName")
    public String getCustomerColumnName() {
        return customerColumnName;
    }

    @JsonProperty("CustomerColumnName")
    public void setCustomerColumnName(String customerColumnName) {
        this.customerColumnName = customerColumnName;
    }

    @JsonProperty("DataSource")
    public String getDataSource() {
        return dataSource;
    }

    @JsonProperty("DataSource")
    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    @JsonProperty("Description")
    public String getDescription() {
        return description;
    }

    @JsonProperty("Description")
    public void setDescription(String description) {
        this.description = description;
    }
}
