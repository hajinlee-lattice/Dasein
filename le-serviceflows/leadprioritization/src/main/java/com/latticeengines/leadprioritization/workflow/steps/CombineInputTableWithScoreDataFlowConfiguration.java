package com.latticeengines.leadprioritization.workflow.steps;

import java.util.List;
import java.util.UUID;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class CombineInputTableWithScoreDataFlowConfiguration extends DataFlowStepConfiguration {

    private List<BucketMetadata> bucketMetadata;
    private String modelType;

    public CombineInputTableWithScoreDataFlowConfiguration() {
        setBeanName("combineInputTableWithScore");
        setTargetTableName("CombineInputTableWithScore_" + UUID.randomUUID().toString());
    }

    public List<BucketMetadata> getBucketMetadata() {
        return this.bucketMetadata;
    }

    public void setBucketMetadata(List<BucketMetadata> bucketMetadataList) {
        this.bucketMetadata = bucketMetadataList;
    }

    public String getModelType() {
        return this.modelType;
    }

    public void setModelType(String modelType) {
        this.modelType = modelType;
    }
}
