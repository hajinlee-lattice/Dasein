package com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

public class CombineInputTableWithScoreParameters extends DataFlowParameters {
    @JsonProperty("score_results_table_name")
    @SourceTableName
    private String scoreResultsTableName;

    @JsonProperty("input_table_name")
    @SourceTableName
    private String inputTableName;

    @JsonProperty("bucket_metadata")
    private List<BucketMetadata> bucketMetadata;

    @JsonProperty("model_type")
    private String modelType;

    public CombineInputTableWithScoreParameters(String scoreResultsTable, String trainingTable) {
        this(scoreResultsTable, trainingTable, null);
    }

    public CombineInputTableWithScoreParameters(String scoreResultsTable, String trainingTable,
            List<BucketMetadata> bucketMetadata) {
        this(scoreResultsTable, trainingTable, bucketMetadata, null);
    }

    public CombineInputTableWithScoreParameters(String scoreResultsTable, String trainingTable,
            List<BucketMetadata> bucketMetadata, String modelType) {
        setScoreResultsTableName(scoreResultsTable);
        setInputTableName(trainingTable);
        setBucketMetadata(bucketMetadata);
        setModelType(modelType);
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public CombineInputTableWithScoreParameters() {
    }

    public String getScoreResultsTableName() {
        return scoreResultsTableName;
    }

    public void setScoreResultsTableName(String scoreResultsTableName) {
        this.scoreResultsTableName = scoreResultsTableName;
    }

    public String getInputTableName() {
        return inputTableName;
    }

    public void setInputTableName(String inputTableName) {
        this.inputTableName = inputTableName;
    }

    public List<BucketMetadata> getBucketMetadata() {
        return this.bucketMetadata;
    }

    public void setBucketMetadata(List<BucketMetadata> bucketMetadata) {
        this.bucketMetadata = bucketMetadata;
    }

    public String getModelType() {
        return this.modelType;
    }

    public void setModelType(String modelType) {
        this.modelType = modelType;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
