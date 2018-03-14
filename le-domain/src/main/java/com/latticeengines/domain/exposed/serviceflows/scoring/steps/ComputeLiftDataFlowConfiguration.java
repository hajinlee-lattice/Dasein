package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import java.util.List;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

public class ComputeLiftDataFlowConfiguration extends BaseScoringDataFlowStepConfiguration {

    private List<BucketMetadata> bucketMetadata;

    private String scoreField;

    public ComputeLiftDataFlowConfiguration() {
        setBeanName("computeLift");
        setTargetTableName(NamingUtils.uuid("ComputeLift"));
    }

    public List<BucketMetadata> getBucketMetadata() {
        return bucketMetadata;
    }

    public void setBucketMetadata(List<BucketMetadata> bucketMetadata) {
        this.bucketMetadata = bucketMetadata;
    }

    public String getScoreField() {
        return scoreField;
    }

    public void setScoreField(String scoreField) {
        this.scoreField = scoreField;
    }
}
