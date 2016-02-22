package com.latticeengines.scoringapi.exposed;

import io.swagger.annotations.ApiModelProperty;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ScoreResponse {

    // TODO buck stops here to ensure scores fall into the correct range
    @JsonProperty("score")
    @ApiModelProperty(required = true)
    private int score;

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

}
