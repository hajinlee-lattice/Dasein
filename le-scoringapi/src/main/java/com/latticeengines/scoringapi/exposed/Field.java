package com.latticeengines.scoringapi.exposed;

import io.swagger.annotations.ApiModelProperty;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.scoringapi.FieldType;

public class Field {

    @JsonProperty("fieldName")
    @ApiModelProperty(required = true, value = "Name of the field")
    private String fieldName;

    @JsonProperty("fieldType")
    @ApiModelProperty(required = true, value = "Data type of the field", allowableValues = "BOOLEAN,INTEGER,FLOAT,STRING,TEMPORAL,LONG")
    private FieldType fieldType;

    @ApiModelProperty(required = true, value = "Value of the field")
    @JsonProperty("fieldValue")
    private Object fieldValue;

    public Field() {
    }

    public Field(String fieldName, FieldType fieldType) {
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
