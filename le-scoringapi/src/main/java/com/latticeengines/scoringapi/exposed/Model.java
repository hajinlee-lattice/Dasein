package com.latticeengines.scoringapi.exposed;

import io.swagger.annotations.ApiModelProperty;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Model {

    @JsonProperty("modelId")
    @ApiModelProperty(required = true, value = "Unique model id")
    private String modelId;

    @JsonProperty("name")
    @ApiModelProperty(value = "User customizable model name")
    private String name;

    @JsonProperty("type")
    @ApiModelProperty(required = true, value = "Model Type", allowableValues = "account, contact")
    private ModelType type;

    public Model() {
    }

    public Model(String modelId, String name, ModelType type) {
        this.modelId = modelId;
        this.name = name;
        this.type = type;
    }

    public String getModelId() {
        return modelId;
    }

    public String getName() {
        return name;
    }

}
