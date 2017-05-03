package com.latticeengines.domain.exposed.scoringapi;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.BucketName;

import io.swagger.annotations.ApiModelProperty;

public class ScoreResponse extends BaseResponse {
    @JsonProperty("id")
    @ApiModelProperty(value = "Record ID", required = true)
    private String id = "";

    @JsonProperty("latticeID")
    @ApiModelProperty(value = "Lattice ID for record", required = true)
    private String latticeId;

    @JsonProperty("score")
    @ApiModelProperty(value = "Score")
    private int score;

    @JsonProperty("rating")
    @ApiModelProperty(value = "Bucket Information Based on Score")
    private BucketName bucket;

    @JsonProperty("classification")
    @ApiModelProperty(value = "Classification")
    private String classification;

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

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public Map<String, Object> getEnrichmentAttributeValues() {
        return enrichmentAttributeValues;
    }

    public void setEnrichmentAttributeValues(Map<String, Object> enrichmentAttributeValues) {
        this.enrichmentAttributeValues = enrichmentAttributeValues;
    }

    public String getClassification() {
        return classification;
    }

    public void setClassification(String classification) {
        this.classification = classification;
    }

    public BucketName getBucket() {
        return this.bucket;
    }

    public void setBucket(BucketName bucket) {
        this.bucket = bucket;
    }

}
