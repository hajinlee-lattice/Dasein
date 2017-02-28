package com.latticeengines.domain.exposed.scoringapi;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

public class Fields {

    @JsonProperty("modelId")
    @ApiModelProperty(required = true, value = "Unique model id")
    private String modelId;

    @JsonProperty("fields")
    @ApiModelProperty(required = true, value = "List of field")
    private List<Field> fields;

    @JsonProperty("validation_expression")
    @ApiModelProperty(required = true, value = "Validation Expression")
    private String validationExpression;

    public Fields() {
        super();
    }

    public Fields(String modelId, List<Field> fields) {
        this();
        this.modelId = modelId;
        this.fields = fields;
    }

    public Fields(String modelId, List<Field> fields, String validationExpression) {
        this();
        this.modelId = modelId;
        this.fields = fields;
        this.validationExpression = validationExpression;
    }

    public String getModelId() {
        return this.modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public List<Field> getFields() {
        return this.fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public String getValidationExpression() {
        return this.validationExpression;
    }

    public void setValidationExpression(String validationExpression) {
        this.validationExpression = validationExpression;
    }

}
