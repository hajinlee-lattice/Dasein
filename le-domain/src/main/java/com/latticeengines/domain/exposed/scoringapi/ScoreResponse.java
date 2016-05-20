package com.latticeengines.domain.exposed.scoringapi;

import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ScoreResponse {
    @JsonProperty("id")
    @ApiModelProperty(value = "Record ID", required = true)
    private String id = "";

    @JsonProperty("latticeID")
    @ApiModelProperty(value = "Lattice ID for record", required = true)
    private String latticeId;

    @JsonProperty("score")
    @ApiModelProperty(value = "Score")
    private double score;

    @JsonProperty("enrichmentAttributeValues")
    @ApiModelProperty(value = "Enrichment attribute values")
    private Map<String, Object> enrichmentAttributeValues;

    @JsonProperty("warnings")
    private List<Warning> warnings = new ArrayList<>();

    @JsonProperty("timestamp")
    @ApiModelProperty(value = "The UTC timestamp of this score in ISO8601 format", required = true)
    private String timestamp = "";

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLatticeId() {
        return latticeId;
    }

    public void setLatticeId(String latticeId) {
        this.latticeId = latticeId;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public Map<String, Object> getEnrichmentAttributeValues() {
        return enrichmentAttributeValues;
    }

    public void setEnrichmentAttributeValues(Map<String, Object> enrichmentAttributeValues) {
        this.enrichmentAttributeValues = enrichmentAttributeValues;
    }

    public List<Warning> getWarnings() {
        return warnings;
    }

    public void setWarnings(List<Warning> warnings) {
        this.warnings = warnings;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
