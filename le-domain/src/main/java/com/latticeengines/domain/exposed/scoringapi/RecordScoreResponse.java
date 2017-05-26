package com.latticeengines.domain.exposed.scoringapi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

import io.swagger.annotations.ApiModelProperty;

public class RecordScoreResponse {
    @JsonProperty("id")
    @ApiModelProperty(value = "Record ID", required = true)
    private String id = "";

    @JsonProperty("latticeID")
    @ApiModelProperty(value = "Lattice ID for record", required = true)
    private String latticeId;

    @JsonProperty("scoreModelTuple")
    @ApiModelProperty(value = "List of score and model tuple")
    private List<ScoreModelTuple> scores;

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

    public List<ScoreModelTuple> getScores() {
        return scores;
    }

    public void setScores(List<ScoreModelTuple> scores) {
        this.scores = scores;
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

    public static class ScoreModelTuple {
        @JsonProperty("modelId")
        @ApiModelProperty(value = "Model ID")
        private String modelId;

        @JsonProperty("score")
        @ApiModelProperty(value = "Score")
        private Integer score;

        @JsonProperty("rating")
        @ApiModelProperty(value = "Bucket Information Based on Score")
        private String bucket;

        @JsonProperty("probability")
        @ApiModelProperty(hidden = true)
        private Double probability;

        @JsonProperty("error")
        @ApiModelProperty(value = "Error")
        private String error;

        @JsonProperty("errorDescription")
        @ApiModelProperty(value = "ErrorDescription")
        private String errorDescription;

        public String getModelId() {
            return modelId;
        }

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public Integer getScore() {
            return score;
        }

        public void setScore(Integer score) {
            this.score = score;
        }

        public Double getProbability() {
            return probability;
        }

        public void setProbability(Double probability) {
            this.probability = probability;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        public String getErrorDescription() {
            return errorDescription;
        }

        public void setErrorDescription(String errorDescription) {
            this.errorDescription = errorDescription;
        }

        public String toString() {
            return JsonUtils.serialize(this);
        }

        public String getBucket() {
            return this.bucket;
        }

        public void setBucket(String bucket) {
            this.bucket = bucket;
        }
    }

    public String toString() {
        return JsonUtils.serialize(this);
    }
}
