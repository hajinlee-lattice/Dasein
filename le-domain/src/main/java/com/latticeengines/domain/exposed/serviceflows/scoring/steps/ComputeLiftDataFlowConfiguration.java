package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.NamingUtils;

public class ComputeLiftDataFlowConfiguration extends BaseScoringDataFlowStepConfiguration {

    @JsonProperty("scoreField")
    private String scoreField;

    @JsonProperty("saveBucketMetadata")
    private Boolean saveBucketMetadata;

    @JsonProperty("ratingEngineId")
    private String ratingEngineId;

    public ComputeLiftDataFlowConfiguration() {
        setBeanName("computeLift");
        setTargetTableName(NamingUtils.uuid("ComputeLift"));
    }

    public String getScoreField() {
        return scoreField;
    }

    public void setScoreField(String scoreField) {
        this.scoreField = scoreField;
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
}
