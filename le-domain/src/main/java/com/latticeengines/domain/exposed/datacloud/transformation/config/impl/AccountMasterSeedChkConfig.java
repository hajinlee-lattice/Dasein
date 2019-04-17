package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AccountMasterSeedChkConfig extends TransformerConfig {
    @JsonProperty("ExceedCountThreshold")
    private long exceededCountThreshold;

    @JsonProperty("CntLessThanThresholdFlag")
    private boolean cntLessThanThresholdFlag;

    @JsonProperty("ID")
    private List<String> id;

    @JsonProperty("GroupByFields")
    private List<String> groupByFields;

    @JsonProperty("Domain")
    private String domain;

    @JsonProperty("DUNS")
    private String duns;

    @JsonProperty("CheckField")
    private String checkField;

    @JsonProperty("Threshold")
    private double threshold;

    @JsonProperty("DomainSource")
    private String domainSource;

    @JsonProperty("ExpectedCoverageFields")
    private List<Object> expectedCoverageFields;

    @JsonProperty("Key")
    private String key;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public long getExceededCountThreshold() {
        return exceededCountThreshold;
    }

    public void setExceededCountThreshold(long exceededCountThreshold) {
        this.exceededCountThreshold = exceededCountThreshold;
    }

    public boolean getLessThanThresholdFlag() {
        return cntLessThanThresholdFlag;
    }

    public void setLessThanThresholdFlag(boolean cntLessThanThresholdFlag) {
        this.cntLessThanThresholdFlag = cntLessThanThresholdFlag;
    }

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
    }

    public List<String> getId() {
        return id;
    }

    public void setId(List<String> id) {
        this.id = id;
    }

    public String getCheckField() {
        return checkField;
    }

    public void setCheckField(String checkField) {
        this.checkField = checkField;
    }

    public String getDomainSource() {
        return domainSource;
    }

    public void setDomainSource(String domainSource) {
        this.domainSource = domainSource;
    }

    public List<Object> getExpectedCoverageFields() {
        return expectedCoverageFields;
    }

    public void setExpectedCoverageFields(List<Object> expectedCoverageFields) {
        this.expectedCoverageFields = expectedCoverageFields;
    }

}
