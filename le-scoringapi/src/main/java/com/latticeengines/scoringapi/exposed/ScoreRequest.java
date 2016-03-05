package com.latticeengines.scoringapi.exposed;

import io.swagger.annotations.ApiModelProperty;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ScoreRequest {

    @JsonProperty("modelId")
    @ApiModelProperty(required = true)
    private String modelId;

    @JsonProperty("record")
    @ApiModelProperty(value = "A record is represented as a JSON Object; ie. { \"field1\" : value1, \"field2\" : value2, .......}", required = true)
    private Map<String, Object> record;

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public Map<String, Object> getRecord() {
        return record;
    }

    public void setRecord(Map<String, Object> record) {
        this.record = record;
    }

}
