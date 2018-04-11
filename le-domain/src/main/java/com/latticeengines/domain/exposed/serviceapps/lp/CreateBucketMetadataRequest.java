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

    @JsonProperty("TenantId")
    private String tenantId;

    @JsonProperty("BucketMetadataList")
    private List<BucketMetadata> bucketMetadataList;

    @JsonProperty("LastModifiedBy")
    private String lastModifiedBy;

    @JsonProperty("ModelGUID")
    private String modelGuid;

    @JsonProperty("RatingEngineId")
    private String ratingEngineId;

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenant) {
        this.tenantId = tenant;
    }

    public List<BucketMetadata> getBucketMetadataList() {
        return bucketMetadataList;
    }

    public void setBucketMetadataList(List<BucketMetadata> bucketMetadataList) {
        this.bucketMetadataList = bucketMetadataList;
    }

    public String getLastModifiedBy() {
        return lastModifiedBy;
    }

    public void setLastModifiedBy(String lastModifiedBy) {
        this.lastModifiedBy = lastModifiedBy;
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
}
