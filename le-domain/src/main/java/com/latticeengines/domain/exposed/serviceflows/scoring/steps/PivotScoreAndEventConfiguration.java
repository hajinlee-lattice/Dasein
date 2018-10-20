package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PivotScoreAndEventConfiguration extends BaseScoringDataFlowStepConfiguration {

    @JsonProperty("eventColumn")
    private String eventColumn;

    @JsonProperty("saveBucketMetadata")
    private Boolean saveBucketMetadata;

    @JsonProperty("ratingEngineId")
    private String ratingEngineId;

    @JsonProperty("scoreField")
    private String scoreField;

    public PivotScoreAndEventConfiguration() {
        setBeanName("pivotScoreAndEvent");
    }

    public String getEventColumn() {
        return eventColumn;
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
}
