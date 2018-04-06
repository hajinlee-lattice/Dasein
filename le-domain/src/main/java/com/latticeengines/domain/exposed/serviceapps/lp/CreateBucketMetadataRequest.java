package com.latticeengines.domain.exposed.serviceapps.lp;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CreateBucketMetadataRequest {

    @JsonProperty("BucketMetadataList")
    List<BucketMetadata> bucketMetadataList;

    @JsonProperty("CreatedBy")
    String createdBy;

    @JsonProperty("ModelGUID")
    String modelGuid;

    @JsonProperty("RatingEngineId")
    String ratingEngineId;

    @JsonProperty("RatingModelId")
    String ratingModelId;

    public List<BucketMetadata> getBucketMetadataList() {
        return bucketMetadataList;
    }

    public void setBucketMetadataList(List<BucketMetadata> bucketMetadataList) {
        this.bucketMetadataList = bucketMetadataList;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getModelGuid() {
        return modelGuid;
    }

    public void setModelGuid(String modelGuid) {
        this.modelGuid = modelGuid;
    }

    public String getRatingEngineId() {
        return ratingEngineId;
    }

    public void setRatingEngineId(String ratingEngineId) {
        this.ratingEngineId = ratingEngineId;
    }

    public String getRatingModelId() {
        return ratingModelId;
    }

    public void setRatingModelId(String ratingModelId) {
        this.ratingModelId = ratingModelId;
    }
}
