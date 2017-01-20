package com.latticeengines.domain.exposed.scoringapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_EMPTY)
public class Field {

    @JsonProperty("fieldName")
    @ApiModelProperty(required = true, value = "Name of the field")
    private String fieldName;

    @JsonProperty("fieldType")
    @ApiModelProperty(required = true, value = "Data type of the field")
    private FieldType fieldType;

    @JsonProperty("displayName")
    @ApiModelProperty(required = false, value = "Display name of the field")
    private String displayName;

    @JsonProperty("isPrimary")
    @ApiModelProperty(required = false, value = "Represents required field mapping for Model")
    private boolean isPrimaryField;

    public Field() {
        super();
    }

    public Field(String fieldName, FieldType fieldType) {
        this();
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    public Field(String fieldName, FieldType fieldType, String displayName) {
        this(fieldName, fieldType);
        this.displayName = displayName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("isPrimary")
    public boolean isPrimaryField() {
        return isPrimaryField;
    }

    public void setPrimaryField(boolean isPrimaryField) {
        this.isPrimaryField = isPrimaryField;
    }
}
