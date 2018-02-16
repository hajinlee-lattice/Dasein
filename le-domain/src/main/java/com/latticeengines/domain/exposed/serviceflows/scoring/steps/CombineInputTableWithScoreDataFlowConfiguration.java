package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

public class CombineInputTableWithScoreDataFlowConfiguration extends BaseScoringDataFlowStepConfiguration {

    private List<BucketMetadata> bucketMetadata;
    private String modelType;

    private boolean liftChart;
    private boolean cdlModel; //M19: to be merged into cdlMultiModel mode in future release ~ M20
    private boolean cdlMultiModel;
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

    @JsonProperty
    public boolean isCdlMultiModel() {
        return cdlMultiModel;
    }

    public void setCdlMultiModel(boolean cdlMultiModel) {
        this.cdlMultiModel = cdlMultiModel;
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
