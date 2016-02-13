package com.latticeengines.scoringapi.exposed;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.wordnik.swagger.annotations.ApiModelProperty;

public class Field {

    @JsonProperty("fieldName")
    @ApiModelProperty(required = true)
    private String fieldName;

    @JsonProperty("fieldType")
    @ApiModelProperty(value = "Data type of the field", allowableValues = "boolean,integer,float,string,temporal,long")
    private FieldType fieldType;

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
