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

    public Fields() {
        super();
    }

    public Fields(String modelId, List<Field> fields) {
        this();
        this.modelId = modelId;
        this.fields = fields;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

}
