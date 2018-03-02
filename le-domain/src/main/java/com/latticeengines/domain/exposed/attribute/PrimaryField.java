package com.latticeengines.domain.exposed.attribute;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PrimaryField {

    public static final String FIELD_TYPE_STRING = "STRING";

    @JsonProperty("fieldName")
    @ApiModelProperty(required = true, value = "Name of the field")
    private String fieldName;

    @JsonProperty("fieldType")
    @ApiModelProperty(required = true, value = "Data type of the field")
    private String fieldType;

    @JsonProperty("displayName")
    @ApiModelProperty(required = false, value = "Display name of the field")
    private String displayName;

    @JsonProperty("externalSystemName")
    @ApiModelProperty(required = false, value = "Customer provided name of the external system")
    private String externalSystemName;

    public PrimaryField() {
        super();
    }

    public PrimaryField(String fieldName, String fieldType) {
        this();
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    public PrimaryField(String fieldName, String fieldType, String displayName) {
        this(fieldName, fieldType);
        this.displayName = displayName;
    }

    public PrimaryField(String fieldName, String fieldType, String displayName, String externalSystemName) {
        this(fieldName, fieldType);
        this.displayName = displayName;
        this.externalSystemName = externalSystemName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getExternalSystemName() {
        return externalSystemName;
    }
}
