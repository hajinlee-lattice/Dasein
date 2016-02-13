package com.latticeengines.scoringapi.exposed;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wordnik.swagger.annotations.ApiModelProperty;

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
