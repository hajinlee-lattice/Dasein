package com.latticeengines.scoringapi.exposed;

import io.swagger.annotations.ApiModelProperty;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DebugScoreResponse extends ScoreResponse {

    @JsonProperty("probability")
    @ApiModelProperty(required = true)
    private double probability;

    @JsonProperty("transformedRecord")
    @ApiModelProperty(required = true)
    private Map<String, Object> transformedRecord;

    public Map<String, Object> getTransformedRecord() {
        return transformedRecord;
    }

    public void setTransformedRecord(Map<String, Object> transformedRecord) {
        this.transformedRecord = transformedRecord;
    }

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }

}
