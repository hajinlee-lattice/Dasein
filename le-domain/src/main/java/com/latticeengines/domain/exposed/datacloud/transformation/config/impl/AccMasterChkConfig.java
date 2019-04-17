package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AccMasterChkConfig extends TransformerConfig {
    @JsonProperty("Key")
    private String key;

    @JsonProperty("GroupByFields")
    private List<String> groupByFields;

    @JsonProperty("Domain")
    private String domain;

    @JsonProperty("Duns")
    private String duns;

    @JsonProperty("ID")
    private List<String> id;

    @JsonProperty("ExceedCountThreshold")
    private long exceededCountThreshold;

    @JsonProperty("CntLessThanThresholdFlag")
    private boolean cntLessThanThresholdFlag;

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }

    public List<String> getId() {
        return id;
    }

    public void setId(List<String> id) {
        this.id = id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
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
}
