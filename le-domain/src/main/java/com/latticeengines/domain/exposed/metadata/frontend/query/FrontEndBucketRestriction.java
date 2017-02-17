package com.latticeengines.domain.exposed.metadata.frontend.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FrontEndBucketRestriction {
    @JsonProperty("attribute_name")
    private String attributeName;

    @JsonProperty("bucket_id")
    private int bucketId;

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public int getBucketId() {
        return bucketId;
    }

    public void setBucketId(int bucketId) {
        this.bucketId = bucketId;
    }
}
