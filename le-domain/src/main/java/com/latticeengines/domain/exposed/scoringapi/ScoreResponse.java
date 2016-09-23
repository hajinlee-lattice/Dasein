package com.latticeengines.domain.exposed.scoringapi;

import io.swagger.annotations.ApiModelProperty;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ScoreResponse extends BaseResponse {
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

}
