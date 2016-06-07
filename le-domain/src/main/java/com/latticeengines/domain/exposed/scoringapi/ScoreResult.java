package com.latticeengines.domain.exposed.scoringapi;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ScoreResult {

    @JsonProperty("Id")
    private String id = "";

    @JsonProperty("modelId")
    private String modelId;

    @JsonProperty("score")
    private double score;

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getModelId() {
        return this.modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public double getScore() {
        return this.score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
