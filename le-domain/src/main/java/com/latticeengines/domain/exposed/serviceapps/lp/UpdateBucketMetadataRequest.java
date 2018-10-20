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
public class UpdateBucketMetadataRequest {

    @JsonProperty("ModelGUID")
    private String modelGuid;

    @JsonProperty("BucketMetadataList")
    private List<BucketMetadata> bucketMetadataList;

    @JsonProperty("IsPublished")
    private boolean isPublished;

    public List<BucketMetadata> getBucketMetadataList() {
        return bucketMetadataList;
    }

    public void setBucketMetadataList(List<BucketMetadata> bucketMetadataList) {
        this.bucketMetadataList = bucketMetadataList;
    }

    public String getModelGuid() {
        return modelGuid;
    }

    public void setModelGuid(String modelGuid) {
        this.modelGuid = modelGuid;
    }

    public boolean isPublished() {
        return isPublished;
    }

    public void setPublished(boolean published) {
        isPublished = published;
    }
}
