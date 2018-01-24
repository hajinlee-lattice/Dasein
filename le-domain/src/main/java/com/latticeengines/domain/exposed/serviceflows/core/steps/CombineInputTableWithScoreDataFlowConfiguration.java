package com.latticeengines.domain.exposed.serviceflows.core.steps;

import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

public class CombineInputTableWithScoreDataFlowConfiguration extends DataFlowStepConfiguration {

    private List<BucketMetadata> bucketMetadata;
    private String modelType;

    private boolean liftChart;
    private boolean cdlModel;
    private boolean expectedValue;

    public CombineInputTableWithScoreDataFlowConfiguration() {
        setBeanName("combineInputTableWithScore");
        setTargetTableName("CombineInputTableWithScore_" + UUID.randomUUID().toString());
    }

    @JsonProperty
    public boolean isCdlModel() {
        return cdlModel;
    }

    public void setCdlModel(boolean cdlModel) {
        this.cdlModel = cdlModel;
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

    public boolean isLiftChart() {
        return liftChart;
    }

    public void setLiftChart(boolean liftChart) {
        this.liftChart = liftChart;
    }

    public boolean isExpectedValue() {
        return expectedValue;
    }

    public void setExpectedValue(boolean expectedValue) {
        this.expectedValue = expectedValue;
    }

}
