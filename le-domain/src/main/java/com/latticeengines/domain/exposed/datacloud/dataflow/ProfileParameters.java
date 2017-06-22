package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProfileParameters extends TransformationFlowParameters {
    @JsonProperty("NumBucketEqualSized")
    private boolean numBucketEqualSized;// true: bucket size is roughly equal
                                        // false: decide bucket upon
                                        // distribution

    @JsonProperty("BucketNum")
    private int bucketNum = 5;// roughly bucket number (might not be exactly
                              // same in final profiling)

    @JsonProperty("MinBucketSize")
    private int minBucketSize = 10; // only for numBucketEqualSized = false

    @JsonProperty("RandSeed")
    private Long randSeed; // used for testing purpose, leave it null for real
                           // use case

    @JsonProperty("EncAttrPrefix")
    private String encAttrPrefix; // used for testing purpose, leave it null for
                                  // real use case

    @JsonProperty("IDAttr")
    private String idAttr;

    @JsonProperty("NumericAttrs")
    private List<Attribute> numericAttrs;

    @JsonProperty("AMAttrsToEnc")
    private List<Attribute> amAttrsToEnc;

    @JsonProperty("ExternalAttrsToEnc")
    private List<Attribute> exAttrsToEnc;

    @JsonProperty("AttrsToRetain")
    private List<Attribute> attrsToRetain;

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

    public Long getRandSeed() {
        return randSeed;
    }

    public void setRandSeed(Long randSeed) {
        this.randSeed = randSeed;
    }

    public String getEncAttrPrefix() {
        return encAttrPrefix;
    }

    public void setEncAttrPrefix(String encAttrPrefix) {
        this.encAttrPrefix = encAttrPrefix;
    }

    public String getIdAttr() {
        return idAttr;
    }

    public void setIdAttr(String idAttr) {
        this.idAttr = idAttr;
    }

    public List<Attribute> getNumericAttrs() {
        return numericAttrs;
    }

    public void setNumericAttrs(List<Attribute> numericAttrs) {
        this.numericAttrs = numericAttrs;
    }

    public List<Attribute> getAmAttrsToEnc() {
        return amAttrsToEnc;
    }

    public void setAmAttrsToEnc(List<Attribute> amAttrsToEnc) {
        this.amAttrsToEnc = amAttrsToEnc;
    }

    public List<Attribute> getExAttrsToEnc() {
        return exAttrsToEnc;
    }

    public void setExAttrsToEnc(List<Attribute> exAttrsToEnc) {
        this.exAttrsToEnc = exAttrsToEnc;
    }

    public List<Attribute> getAttrsToRetain() {
        return attrsToRetain;
    }

    public void setAttrsToRetain(List<Attribute> attrsToRetain) {
        this.attrsToRetain = attrsToRetain;
    }

    public static class Attribute {
        private String attr;
        private Integer encodeBitUnit;
        private String decodeStrategy;
        private BucketAlgorithm algo;

        public Attribute(String attr, Integer encodeBitUnit, String decodeStrategy, BucketAlgorithm algo) {
            this.attr = attr;
            this.encodeBitUnit = encodeBitUnit;
            this.decodeStrategy = decodeStrategy;
            this.algo = algo;
        }

        public String getAttr() {
            return attr;
        }

        public void setAttr(String attr) {
            this.attr = attr;
        }

        public Integer getEncodeBitUnit() {
            return encodeBitUnit;
        }

        public void setEncodeBitUnit(Integer encodeBitUnit) {
            this.encodeBitUnit = encodeBitUnit;
        }

        public BucketAlgorithm getAlgo() {
            return algo;
        }

        public void setAlgo(BucketAlgorithm algo) {
            this.algo = algo;
        }

        public String getDecodeStrategy() {
            return decodeStrategy;
        }

        public void setDecodeStrategy(String decodeStrategy) {
            this.decodeStrategy = decodeStrategy;
        }

    }

}
