package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LeadEnrichmentAttribute {

    private String displayName;
    private String fieldName;
    private String fieldType;
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

    @JsonProperty("Description")
    public String getDescription() {
        return description;
    }

    @JsonProperty("Description")
    public void setDescription(String description) {
        this.description = description;
    }
}
