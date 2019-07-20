package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PivotScoreAndEventConfiguration extends BaseScoringDataFlowStepConfiguration {

    @JsonProperty("isEV")
    private boolean isEV;

    @JsonProperty("eventColumn")
    private String eventColumn;

    @JsonProperty("saveBucketMetadata")
    private Boolean saveBucketMetadata;

    @JsonProperty("ratingEngineId")
    private String ratingEngineId;

    @JsonProperty("scoreField")
    private String scoreField;

    @JsonProperty("target_score_derivation")
    private boolean targetScoreDerivation;
    
    public PivotScoreAndEventConfiguration() {
        setBeanName("pivotScoreAndEvent");
    }

    public boolean isEV() {
        return isEV;
    }

    public void setEV(boolean isEV) {
        this.isEV = isEV;
    }

    public void setEventColumn(String eventColumn) {
        this.eventColumn = eventColumn;
    }

    public Boolean getSaveBucketMetadata() {
        return saveBucketMetadata;
    }

    public void setSaveBucketMetadata(Boolean saveBucketMetadata) {
        this.saveBucketMetadata = saveBucketMetadata;
    }

    public String getRatingEngineId() {
        return ratingEngineId;
    }

    public void setRatingEngineId(String ratingEngineId) {
        this.ratingEngineId = ratingEngineId;
    }

    public String getScoreField() {
        return scoreField;
    }

    public void setScoreField(String scoreField) {
        this.scoreField = scoreField;
    }

    public boolean isTargetScoreDerivation() {
        return targetScoreDerivation;
    }

    public void setTargetScoreDerivation(boolean targetScoreDerivation) {
        this.targetScoreDerivation = targetScoreDerivation;
    }
}
