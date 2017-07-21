package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BucketInformation {

    @JsonProperty("bucket")
    private String bucket;

    @JsonProperty("bucketCount")
    private int bucketCount;

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getBucket() {
        return this.bucket;
    }

    public void setBucketCount(int bucketCount) {
        this.bucketCount = bucketCount;
    }

    public int getBucketCount() {
        return this.bucketCount;
    }
}
