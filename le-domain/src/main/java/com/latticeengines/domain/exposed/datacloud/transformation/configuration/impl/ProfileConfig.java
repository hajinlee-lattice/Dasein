package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProfileConfig extends TransformerConfig {

    // Be cautious: if both data size and sample rate are large, it could
    // potentially cause OOM
    @JsonProperty("SampleRate")
    private double sampleRate = 0.0001;

    @JsonProperty("NumBucketEqualSized")
    private boolean numBucketEqualSized;    // true: bucket size is roughly equal  false: decide bucket upon distribution

    @JsonProperty("BucketNum")
    private int bucketNum = 5;

    @JsonProperty("MinBucketSize")
    private int minBucketSize = 10; // used for numBucketEqualSized = false

    public double getSampleRate() {
        return sampleRate;
    }

    public void setSampleRate(double sampleRate) {
        this.sampleRate = sampleRate;
    }

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
