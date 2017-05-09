package com.latticeengines.domain.exposed.pls;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.domain.exposed.metadata.FundamentalType;

@JsonIgnoreType
public class LeadEnrichmentAttribute implements HasAttributeCustomizations {

    @JsonProperty("DisplayName")
    private String displayName;

    @JsonProperty("FieldName")
    private String fieldName;

    @JsonProperty("FieldNameInTarget")
    private String fieldNameInTarget;

    @JsonProperty("FieldType")
    private String fieldType;

    @JsonProperty("FieldJavaType")
    private String fieldJavaType;

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

    @JsonProperty("Subcategory")
    private String subcategory;

    @JsonProperty("IsInternal")
    private Boolean isInternal;

    @JsonProperty("FundamentalType")
    private FundamentalType fundamentalType;

    @JsonProperty("AttributeFlagsMap")
    private Map<AttributeUseCase, JsonNode> attributeFlagsMap;

    @JsonProperty("ImportanceOrdering")
    private int importanceOrdering;

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

    public String getFieldJavaType() {
        return fieldJavaType;
    }

    public void setFieldJavaType(String fieldJavaType) {
        this.fieldJavaType = fieldJavaType;
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

    public String getSubcategory() {
        return subcategory;
    }

    public void setSubcategory(String subcategory) {
        this.subcategory = subcategory;
    }

    public Boolean getIsInternal() {
        return isInternal;
    }

    public void setIsInternal(Boolean isInternal) {
        this.isInternal = isInternal;
    }

    public FundamentalType getFundamentalType() {
        return fundamentalType;
    }

    public void setFundamentalType(FundamentalType fundamentalType) {
        this.fundamentalType = fundamentalType;
    }

    public Map<AttributeUseCase, JsonNode> getAttributeFlagsMap() {
        return attributeFlagsMap;
    }

    public void setAttributeFlagsMap(Map<AttributeUseCase, JsonNode> attributeFlagsMap) {
        this.attributeFlagsMap = attributeFlagsMap;
    }

    public int getImportanceOrdering() {
        return importanceOrdering;
    }

    public void setImportanceOrdering(int importanceOrdering) {
        this.importanceOrdering = importanceOrdering;
    }

    @Override
    @JsonIgnore
    public String getCategoryAsString() {
        return category;
    }

    @Override
    @JsonIgnore
    public String getColumnId() {
        return fieldName;
    }
}
