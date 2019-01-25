package com.latticeengines.domain.exposed.serviceflows.scoring.dataflow;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;

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

    @JsonProperty("score_field_name")
    private String scoreFieldName = ScoreResultField.Percentile.displayName;

    @JsonProperty("score_multiplier")
    private Integer scoreMultiplier;

    @JsonProperty("avg_score")
    private Double avgScore;

    @JsonProperty("id_column")
    private String idColumn = InterfaceName.Id.name();

    // params for multi model
    @JsonProperty("bucket_metadata_map")
    private Map<String, List<BucketMetadata>> bucketMetadataMap;

    @JsonProperty("model_id_field")
    private String modelIdField;

    public CombineInputTableWithScoreParameters(String scoreResultsTable, String trainingTable) {
        this(scoreResultsTable, trainingTable, null);
    }

    public CombineInputTableWithScoreParameters(String scoreResultsTable, String trainingTable,
            List<BucketMetadata> bucketMetadata) {
        this(scoreResultsTable, trainingTable, bucketMetadata, null, InterfaceName.InternalId.name());
    }

    public CombineInputTableWithScoreParameters(String scoreResultsTable, String trainingTable,
            List<BucketMetadata> bucketMetadata, String modelType, String idColumn) {
        setScoreResultsTableName(scoreResultsTable);
        setInputTableName(trainingTable);
        setBucketMetadata(bucketMetadata);
        setModelType(modelType);
        setIdColumn(idColumn);
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

    public String getScoreFieldName() {
        return scoreFieldName;
    }

    public void setScoreFieldName(String scoreFieldName) {
        this.scoreFieldName = scoreFieldName;
    }

    public Integer getScoreMultiplier() {
        return scoreMultiplier;
    }

    public void setScoreMultiplier(Integer scoreMultiplier) {
        this.scoreMultiplier = scoreMultiplier;
    }

    public Double getAvgScore() {
        return avgScore;
    }

    public void setAvgScore(Double avgScore) {
        this.avgScore = avgScore;
    }

    public String getIdColumn() {
        return idColumn;
    }

    public void setIdColumn(String idColumn) {
        this.idColumn = idColumn;
    }

    public Map<String, List<BucketMetadata>> getBucketMetadataMap() {
        return bucketMetadataMap;
    }

    public void setBucketMetadataMap(Map<String, List<BucketMetadata>> bucketMetadataMap) {
        this.bucketMetadataMap = bucketMetadataMap;
    }

    public String getModelIdField() {
        return modelIdField;
    }

    public void setModelIdField(String modelIdField) {
        this.modelIdField = modelIdField;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
