package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

public class ProfileConfig extends TblDrivenTransformerConfig {
    @JsonProperty("NumBucketEqualSized")
    private boolean numBucketEqualSized;// true: bucket size is roughly equal
    @JsonProperty("BucketNum")
    private int bucketNum = 5;// roughly bucket number (might not be exactly
                                        // false: decide bucket upon
                                        // distribution
    @JsonProperty("MinBucketSize")
    private int minBucketSize = 10; // only for numBucketEqualSized = false
                              // same in final profiling)
    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion; // by default, segmentation: use current
    @JsonProperty("RandSeed")
    private Long randSeed; // used for testing purpose, leave it null for real
                                     // approved version; enrichment: use a
                                     // version next to current approved version
    @JsonProperty("EncAttrPrefix")
    private String encAttrPrefix; // used for testing purpose, leave it null for
                           // use case
    @JsonProperty("MaxCat")
    private int maxCat = 2048; // Maximum allowed category number
                                  // real use case
    @JsonProperty("MaxCatLen")
    private int maxCatLength = 1024; // Maximum allowed category attribute
    @JsonProperty("CatAttrsNotEnc")
    private String[] catAttrsNotEnc; // Dimensional attributes for stats should
                                     // length. If exceeded, this attribute is
                                     // not segmentable
    @JsonProperty("MaxDiscrete")
    private int maxDiscrete = 5; // Maximum allowed discrete bucket number
                                     // not be encoded

    @JsonProperty("EvaluationDateAsTimestamp")
    private long evaluationDateAsTimestamp = -1; // Timestamp the PA job is run for use for Date Attribute profiling.

    public ProfileConfig() {

    }

    @Override
    public String getStage() {
        if (StringUtils.isBlank(super.getStage())) {
            super.setStage(DataCloudConstants.PROFILE_STAGE_SEGMENT);
        }
        return super.getStage();
    }

    @Override
    public String getTransformer() {
        return DataCloudConstants.TRANSFORMER_PROFILER;
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

    public int getMinBucketSize() { return minBucketSize; }

    public void setMinBucketSize(int minBucketSize) {
        this.minBucketSize = minBucketSize;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
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

    public int getMaxCat() {
        return maxCat;
    }

    public void setMaxCat(int maxCat) {
        this.maxCat = maxCat;
    }

    public int getMaxCatLength() {
        return maxCatLength;
    }

    public void setMaxCatLength(int maxCatLength) {
        this.maxCatLength = maxCatLength;
    }

    public String[] getCatAttrsNotEnc() {
        return catAttrsNotEnc;
    }

    public void setCatAttrsNotEnc(String[] catAttrsNotEnc) {
        this.catAttrsNotEnc = catAttrsNotEnc;
    }

    public int getMaxDiscrete() {
        return maxDiscrete;
    }

    public void setMaxDiscrete(int maxDiscrete) {
        this.maxDiscrete = maxDiscrete;
    }

    public long getEvaluationDateAsTimestamp() {
        return evaluationDateAsTimestamp;
    }

    public void setEvaluationDateAsTimestamp(long evaluationDateAsTimestamp) {
        this.evaluationDateAsTimestamp = evaluationDateAsTimestamp;
    }

}
