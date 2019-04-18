package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RatingEngineActionConfiguration extends ActionConfiguration {

    @JsonProperty("ratingEngineId")
    private String ratingEngineId;
    @JsonProperty("modelId")
    private String modelId;
    @JsonProperty("subType")
    private SubType subType;

    public RatingEngineActionConfiguration() {
    }

    public String getRatingEngineId() {
        return this.ratingEngineId;
    }

    public void setRatingEngineId(String ratingEngineId) {
        this.ratingEngineId = ratingEngineId;
    }

    public String getModelId() {
        return this.modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public SubType getSubType() {
        return this.subType;
    }

    public void setSubType(SubType subType) {
        this.subType = subType;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    public String serialize() {
        switch (this.subType) {
            case ACTIVATION:
                return String.format(SubType.ACTIVATION.getFormat(), this.ratingEngineId);
            case DELETION:
                return String.format(SubType.DELETION.getFormat(), this.ratingEngineId);
            case AI_MODEL_BUCKET_CHANGE:
                return String.format(SubType.AI_MODEL_BUCKET_CHANGE.getFormat(), this.modelId,
                        this.ratingEngineId);
            case RULE_MODEL_BUCKET_CHANGE:
                return String.format(SubType.RULE_MODEL_BUCKET_CHANGE.getFormat(), this.modelId,
                        this.ratingEngineId);
            default:
                return toString();
        }
    }

    public enum SubType {

        ACTIVATION("Rating Engine %s is activated."), //
        DELETION("Rating Engine %s has been deleted."), //
        AI_MODEL_BUCKET_CHANGE(
                "Rating Buckets of Rating Model %s for Rating Engine %s has been updated."), //
        RULE_MODEL_BUCKET_CHANGE(
                "Rating Rule of Rating Model %s for Rating Engine %s has been updated.");

        private String format;

        SubType(String format) {
            this.format = format;
        }

        public String getFormat() {
            return this.format;
        }

    }

}
