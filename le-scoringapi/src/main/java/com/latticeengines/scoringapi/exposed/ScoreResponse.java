package com.latticeengines.scoringapi.exposed;

import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.scoringapi.exposed.warnings.Warning;

public class ScoreResponse {

    @JsonProperty("score")
    @ApiModelProperty(required = true)
    private double score;

    @JsonProperty("warnings")
    private List<Warning> warnings = new ArrayList<>();

    @JsonProperty("id")
    @ApiModelProperty(required = true)
    private String id = "";

    @JsonProperty("timestamp")
    @ApiModelProperty(value = "The UTC timestamp of this score in ISO8601 format", required = true)
    private String timestamp = "";

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

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
