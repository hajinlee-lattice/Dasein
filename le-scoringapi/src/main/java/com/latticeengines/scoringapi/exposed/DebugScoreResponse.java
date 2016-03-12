package com.latticeengines.scoringapi.exposed;

import io.swagger.annotations.ApiModelProperty;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DebugScoreResponse extends ScoreResponse {

    @JsonProperty("probability")
    @ApiModelProperty(required = true)
    private double probability;

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }

}
