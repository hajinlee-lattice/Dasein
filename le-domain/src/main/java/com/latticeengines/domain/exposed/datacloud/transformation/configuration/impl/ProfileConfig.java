package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProfileConfig extends TransformerConfig {

    @JsonProperty("NumBucketEqualSized")
    private boolean numBucketEqualSized;    // true: bucket size is roughly equal  false: decide bucket upon distribution

    @JsonProperty("BucketNum")
    private int bucketNum = 5;  // roughly bucket number (might not be exactly same in final profiling)

    @JsonProperty("MinBucketSize")
    private int minBucketSize = 10; // used for numBucketEqualSized = false

    public boolean isNumBucketEqualSized() {
        return numBucketEqualSized;
    }

    public void setNumBucketEqualSized(boolean numBucketEqualSized) {
        this.numBucketEqualSized = numBucketEqualSized;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public void setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
    }

    public int getMinBucketSize() {
        return minBucketSize;
    }

    public void setMinBucketSize(int minBucketSize) {
        this.minBucketSize = minBucketSize;
    }

}
