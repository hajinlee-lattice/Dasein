package com.latticeengines.domain.exposed.spark.stats;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ProfileJobConfig extends SparkJobConfig {

    public static final String NAME = "profile";

    @JsonProperty("Stage")
    private String stage;

    @JsonProperty("EvaluationDateAsTimestamp")
    private long evaluationDateAsTimestamp = -1; // Timestamp the PA job is run for use for Date Attribute profiling.

    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion; // by default, segmentation: use current

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

    @JsonProperty("MaxCat")
    private int maxCat = 2048; // Maximum allowed category number

    @JsonProperty("MaxCatLen")
    private int maxCatLength = 1024; // Maximum allowed category attribute
    // length. If exceeded, this attribute is
    // not segmentable

    @JsonProperty("CatAttrsNotEnc")
    private String[] catAttrsNotEnc; // Dimensional attributes for stats should
    // not be encoded

    @JsonProperty("MaxDiscrete")
    private int maxDiscrete = 5; // Maximum allowed discrete bucket number

    @JsonProperty("IDAttr")
    private String idAttr;

    @JsonProperty("NumericAttrs")
    private List<ProfileParameters.Attribute> numericAttrs;

    @JsonProperty("CatAttrs")
    private List<ProfileParameters.Attribute> catAttrs;

    @JsonProperty("AMAttrsToEnc")
    private List<ProfileParameters.Attribute> amAttrsToEnc;

    @JsonProperty("ExternalAttrsToEnc")
    private List<ProfileParameters.Attribute> exAttrsToEnc;

    @JsonProperty("CodeBookMap")
    private Map<String, BitCodeBook> codeBookMap; // encoded attr -> bitCodeBook

    @JsonProperty("CodeBookLookup")
    private Map<String, String> codeBookLookup; // decoded attr -> encoded attr

    @JsonProperty("EnforceProfileByAttr")
    private Boolean enforceProfileByAttr; // for test: enforce profile attr-by-attr even for small data set

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public String getStage() {
        if (StringUtils.isBlank(stage)) {
            setStage(DataCloudConstants.PROFILE_STAGE_SEGMENT);
        }
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
    }

    public long getEvaluationDateAsTimestamp() {
        return evaluationDateAsTimestamp;
    }

    public void setEvaluationDateAsTimestamp(long evaluationDateAsTimestamp) {
        this.evaluationDateAsTimestamp = evaluationDateAsTimestamp;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
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

    public String getIdAttr() {
        return idAttr;
    }

    public void setIdAttr(String idAttr) {
        this.idAttr = idAttr;
    }

    public List<ProfileParameters.Attribute> getNumericAttrs() {
        return numericAttrs;
    }

    public void setNumericAttrs(List<ProfileParameters.Attribute> numericAttrs) {
        this.numericAttrs = numericAttrs;
    }

    public List<ProfileParameters.Attribute> getCatAttrs() {
        return catAttrs;
    }

    public void setCatAttrs(List<ProfileParameters.Attribute> catAttrs) {
        this.catAttrs = catAttrs;
    }

    public int getMaxDiscrete() {
        return maxDiscrete;
    }

    public void setMaxDiscrete(int maxDiscrete) {
        this.maxDiscrete = maxDiscrete;
    }

    public List<ProfileParameters.Attribute> getAmAttrsToEnc() {
        return amAttrsToEnc;
    }

    public void setAmAttrsToEnc(List<ProfileParameters.Attribute> amAttrsToEnc) {
        this.amAttrsToEnc = amAttrsToEnc;
    }

    public List<ProfileParameters.Attribute> getExAttrsToEnc() {
        return exAttrsToEnc;
    }

    public void setExAttrsToEnc(List<ProfileParameters.Attribute> exAttrsToEnc) {
        this.exAttrsToEnc = exAttrsToEnc;
    }

    public Map<String, BitCodeBook> getCodeBookMap() {
        return codeBookMap;
    }

    public void setCodeBookMap(Map<String, BitCodeBook> codeBookMap) {
        this.codeBookMap = codeBookMap;
    }

    public Map<String, String> getCodeBookLookup() {
        return codeBookLookup;
    }

    public void setCodeBookLookup(Map<String, String> codeBookLookup) {
        this.codeBookLookup = codeBookLookup;
    }

    public Boolean getEnforceProfileByAttr() {
        return enforceProfileByAttr;
    }

    public void setEnforceProfileByAttr(Boolean enforceProfileByAttr) {
        this.enforceProfileByAttr = enforceProfileByAttr;
    }
}
