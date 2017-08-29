package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

public class ProfileConfig extends TblDrivenTransformerConfig {
    public ProfileConfig() {

    }

    @JsonProperty("NumBucketEqualSized")
    private boolean numBucketEqualSized;// true: bucket size is roughly equal
                                        // false: decide bucket upon distribution

    @JsonProperty("BucketNum")
    private int bucketNum = 5;// roughly bucket number (might not be exactly
                              // same in final profiling)

    @JsonProperty("MinBucketSize")
    private int minBucketSize = 10; // only for numBucketEqualSized = false

    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion; // by default, segmentation: use current
                                     // approved version; enrichment: use a
                                     // version next to current approved version

    @JsonProperty("RandSeed")
    private Long randSeed; // used for testing purpose, leave it null for real use case

    @JsonProperty("EncAttrPrefix")
    private String encAttrPrefix; // used for testing purpose, leave it null for real use case

    @JsonProperty("MaxCat")
    private int maxCat = 2048; // Maximum allowed category number

    @JsonProperty("CatAttrsNotEnc")
    private String[] catAttrsNotEnc; // Dimensional attributes for stats should
                                     // not be encoded

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

    public int getMaxCat() {
        return maxCat;
    }

    public void setMaxCat(int maxCat) {
        this.maxCat = maxCat;
    }

    public String[] getCatAttrsNotEnc() {
        return catAttrsNotEnc;
    }

    public void setCatAttrsNotEnc(String[] catAttrsNotEnc) {
        this.catAttrsNotEnc = catAttrsNotEnc;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

}
