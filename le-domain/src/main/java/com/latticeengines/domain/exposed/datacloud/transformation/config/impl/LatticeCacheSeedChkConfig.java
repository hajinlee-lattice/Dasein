package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LatticeCacheSeedChkConfig extends TransformerConfig {
    @JsonProperty("GroupByFields")
    private List<String> groupByFields;

    @JsonProperty("Domain")
    private String domain;

    @JsonProperty("DUNS")
    private String duns;

    @JsonProperty("ID")
    private List<String> id;

    @JsonProperty("ExceedCountThreshold")
    private long exceededCountThreshold;

    @JsonProperty("CntLessThanThresholdFlag")
    private boolean cntLessThanThresholdFlag;

    @JsonProperty("Threshold")
    private double threshold;

    @JsonProperty("CheckField")
    private String checkField;

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
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

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }
}
