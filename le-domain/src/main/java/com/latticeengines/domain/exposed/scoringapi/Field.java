package com.latticeengines.domain.exposed.scoringapi;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

public class Field {

    @JsonProperty("fieldName")
    @ApiModelProperty(required = true, value = "Name of the field")
    private String fieldName;

    @JsonProperty("fieldType")
    @ApiModelProperty(required = true, value = "Data type of the field")
    private FieldType fieldType;

    public Field() {
        super();
    }

    public Field(String fieldName, FieldType fieldType) {
        this();
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

}
