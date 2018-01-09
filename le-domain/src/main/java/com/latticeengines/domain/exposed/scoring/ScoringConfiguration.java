package com.latticeengines.domain.exposed.scoring;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

public class ScoringConfiguration {

    private String customer;

    private String uniqueKeyColumn;

    private String sourceDataDir;

    private String targetResultDir;

    private List<String> modelGuids = new ArrayList<>();

    private List<BucketMetadata> bucketMetadata;

    private ScoringInputType scoreInputType = ScoringInputType.Json;

    @JsonProperty("customer")
    public String getCustomer() {
        return customer;
    }

    @JsonProperty("customer")
    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @JsonProperty("unique_key_column")
    public String getUniqueKeyColumn() {
        return uniqueKeyColumn;
    }

    @JsonProperty("unique_key_column")
    public void setUniqueKeyColumn(String uniqueKeyColumn) {
        this.uniqueKeyColumn = uniqueKeyColumn;
    }

    @JsonProperty("source_data_dir")
    public String getSourceDataDir() {
        return sourceDataDir;
    }

    @JsonProperty("source_data_dir")
    public void setSourceDataDir(String sourceDataDir) {
        this.sourceDataDir = sourceDataDir;
    }

    @JsonProperty("target_result_dir")
    public String getTargetResultDir() {
        return targetResultDir;
    }

    @JsonProperty("target_result_dir")
    public void setTargetResultDir(String targetResultDir) {
        this.targetResultDir = targetResultDir;
    }

    @JsonProperty("model_guids")
    public List<String> getModelGuids() {
        return modelGuids;
    }

    @JsonProperty("model_guids")
    public void setModelGuids(List<String> modelGuids) {
        this.modelGuids = modelGuids;
    }

    @JsonProperty("bucket_metadata")
    public void setBucketMetadata(List<BucketMetadata> bucketMetadata) {
        this.bucketMetadata = bucketMetadata;
    }

    @JsonProperty("bucket_metadata")
    public List<BucketMetadata> getBucketMetadata() {
        return this.bucketMetadata;
    }

    @JsonProperty("score_input_type")
    public ScoringInputType getScoreInputType() {
        return this.scoreInputType;
    }

    @JsonProperty("score_input_type")
    public void setScoreInputType(ScoringInputType scoreInputType) {
        this.scoreInputType = scoreInputType;
    }

    public String toString() {
        return JsonUtils.serialize(this);
    }

    public enum ScoringInputType {
        Json, //
        Avro;
    }
}
