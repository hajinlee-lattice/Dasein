package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreType
public class LeadEnrichmentAttribute {

    @JsonProperty("DisplayName")
    private String displayName;

    @JsonProperty("FieldName")
    private String fieldName;

    @JsonProperty("FieldNameInTarget")
    private String fieldNameInTarget;

    @JsonProperty("FieldType")
    private String fieldType;

    @JsonProperty("CustomerColumnName")
    private String customerColumnName;

    @JsonProperty("DataSource")
    private String dataSource;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("IsSelected")
    private Boolean isSelected;

    @JsonProperty("IsPremium")
    private Boolean isPremium;

    @JsonProperty("Category")
    private String category;

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldNameInTarget() {
        return fieldNameInTarget;
    }

    public void setFieldNameInTarget(String fieldNameInTarget) {
        this.fieldNameInTarget = fieldNameInTarget;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getCustomerColumnName() {
        return customerColumnName;
    }

    public void setCustomerColumnName(String customerColumnName) {
        this.customerColumnName = customerColumnName;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean getIsSelected() {
        return isSelected;
    }

    public void setIsSelected(Boolean isSelected) {
        this.isSelected = isSelected;
    }

    public Boolean getIsPremium() {
        return isPremium;
    }

    public void setIsPremium(Boolean isPremium) {
        this.isPremium = isPremium;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }
}
