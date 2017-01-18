package com.latticeengines.domain.exposed.validation;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

/**
 * @author jdraadusumalli
 * 
 *         Supports additional metadata for Validation Field. By default any
 *         field that is included in @ValidationExpression is required. This can
 *         be enhanced to include additional validation criteria like,
 *         minlength, maxlength, and other dimentions.
 */
public class ValidationField {

    @JsonProperty("fieldName")
    @ApiModelProperty(required = true, value = "Name of the field")
    private String fieldName;

    public ValidationField() {
        super();
    }

    public ValidationField(String fieldName) {
        super();
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

}
