package com.latticeengines.scoringapi.exposed;

import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.scoringapi.warnings.Warning;

public class ScoreResponse {

    @JsonProperty("score")
    @ApiModelProperty(required = true)
    private double score;

    @JsonProperty("warnings")
    private List<Warning> warnings = new ArrayList<>();

    @JsonProperty("id")
    @ApiModelProperty(required = true)
    private String id = "";

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public List<Warning> getWarnings() {
        return warnings;
    }

    public void setWarnings(List<Warning> warnings) {
        this.warnings = warnings;
    }

}
