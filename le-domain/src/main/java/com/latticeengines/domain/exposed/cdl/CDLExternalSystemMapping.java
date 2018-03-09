package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CDLExternalSystemMapping {

    public static final String FIELD_TYPE_STRING = "STRING";

    @JsonProperty("fieldName")
    private String fieldName;

    @JsonProperty("fieldType")
    private String fieldType;

    @JsonProperty("displayName")
    private String displayName;

    public String getFieldName() {
        return fieldName;
    }

    public CDLExternalSystemMapping() {

    }

    public CDLExternalSystemMapping(String fieldName, String fieldType, String displayName) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.displayName = displayName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }
}
