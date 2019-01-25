package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

public class CombineInputTableWithScoreDataFlowConfiguration
        extends BaseScoringDataFlowStepConfiguration {

    @JsonProperty
    private List<BucketMetadata> bucketMetadata;

    @JsonProperty
    private String modelType;

    @JsonProperty
    private boolean liftChart;

    @JsonProperty
    private boolean cdlModel; // M19: to be merged into cdlMultiModel mode in
                              // future release ~ M20
    @JsonProperty
    private boolean cdlMultiModel;

    @JsonProperty
    private boolean expectedValue;

    @JsonProperty
    private String idColumnName = InterfaceName.InternalId.name();

    public CombineInputTableWithScoreDataFlowConfiguration() {
        setBeanName("combineInputTableWithScore");
        setTargetTableName(NamingUtils.uuid("CombineInputTableWithScore"));
    }

    public boolean isCdlModel() {
        return cdlModel;
    }

    public void setCdlModel(boolean cdlModel) {
        this.cdlModel = cdlModel;
    }

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

    public String getIdColumnName() {
        return idColumnName;
    }

    public void setIdColumnName(String idColumnName) {
        this.idColumnName = idColumnName;
    }

}
