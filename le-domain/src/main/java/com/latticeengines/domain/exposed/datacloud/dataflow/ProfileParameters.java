package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProfileParameters extends TransformationFlowParameters {
    @JsonProperty("NumBucketEqualSized")
    private boolean numBucketEqualSized;// true: bucket size is roughly equal
                                        // false: decide bucket upon distribution

    @JsonProperty("BucketNum")
    private int bucketNum = 5;// roughly bucket number (might not be exactly same in final profiling)

    @JsonProperty("MinBucketSize")
    private int minBucketSize = 10; // only for numBucketEqualSized = false

    @JsonProperty("NumAttrs")
    private List<String> numAttrs;  // attrs for interval bucket

    @JsonProperty("BoolAttrs")
    private List<String> boolAttrs; // attrs for boolean bucket

    @JsonProperty("EncodedAttrs")
    private Map<String, Pair<Integer, Map<String, String>>> encodedAttrs; // encoded attrs (encoded attr->(bit unit, attr->decode strategy))

    @JsonProperty("RetainedAttrs")
    private List<String> retainedAttrs; // retained attrs without bucket

    @JsonProperty("RandSeed")
    private Long randSeed; // used for testing purpose

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

    public List<String> getNumAttrs() {
        return numAttrs;
    }

    public void setNumAttrs(List<String> numAttrs) {
        this.numAttrs = numAttrs;
    }

    public List<String> getBoolAttrs() {
        return boolAttrs;
    }

    public void setBoolAttrs(List<String> boolAttrs) {
        this.boolAttrs = boolAttrs;
    }

    public Map<String, Pair<Integer, Map<String, String>>> getEncodedAttrs() {
        return encodedAttrs;
    }

    public void setEncodedAttrs(Map<String, Pair<Integer, Map<String, String>>> encodedAttrs) {
        this.encodedAttrs = encodedAttrs;
    }

    public List<String> getRetainedAttrs() {
        return retainedAttrs;
    }

    public void setRetainedAttrs(List<String> retainedAttrs) {
        this.retainedAttrs = retainedAttrs;
    }

    public Long getRandSeed() {
        return randSeed;
    }

    public void setRandSeed(Long randSeed) {
        this.randSeed = randSeed;
    }

}
