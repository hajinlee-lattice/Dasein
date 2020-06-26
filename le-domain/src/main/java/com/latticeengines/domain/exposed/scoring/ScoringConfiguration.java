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

    private boolean useScorederivation;

    private List<String> modelGuids = new ArrayList<>();

    private List<String> p2ModelGuids = new ArrayList<>();

    private List<BucketMetadata> bucketMetadata;

    private boolean readModelIdFromRecord = true;

    private ScoringInputType scoreInputType = ScoringInputType.Json;

    private String pythonMajorVersion;

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

    @JsonProperty("p2_model_guids")
    public List<String> getP2ModelGuids() {
        return p2ModelGuids;
    }

    @JsonProperty("p2_model_guids")
    public void setP2ModelGuids(List<String> p2ModelGuids) {
        this.p2ModelGuids = p2ModelGuids;
    }

    @JsonProperty("bucket_metadata")
    public List<BucketMetadata> getBucketMetadata() {
        return this.bucketMetadata;
    }

    @JsonProperty("bucket_metadata")
    public void setBucketMetadata(List<BucketMetadata> bucketMetadata) {
        this.bucketMetadata = bucketMetadata;
    }

    @JsonProperty("score_input_type")
    public ScoringInputType getScoreInputType() {
        return this.scoreInputType;
    }

    @JsonProperty("score_input_type")
    public void setScoreInputType(ScoringInputType scoreInputType) {
        this.scoreInputType = scoreInputType;
    }

    @JsonProperty("use_score_derivation")
    public boolean getUseScorederivation() {
        return useScorederivation;
    }

    @JsonProperty("use_score_derivation")
    public void setUseScorederivation(Boolean useScorederivation) {
        this.useScorederivation = useScorederivation;
    }

    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonProperty("read_modelid_from_record")
    public boolean readModelIdFromRecord() {
        return readModelIdFromRecord;
    }

    @JsonProperty("python_major_version")
    public String getPythonMajorVersion() {
        return pythonMajorVersion;
    }

    @JsonProperty("python_major_version")
    public void setPythonMajorVersion(String pythonMajorVersion) {
        this.pythonMajorVersion = pythonMajorVersion;
    }

    public void setModelIdFromRecord(boolean readModelIdFromRecord) {
        this.readModelIdFromRecord = readModelIdFromRecord;
    }

    public enum ScoringInputType {
        Json, //
        Avro;
    }

}
