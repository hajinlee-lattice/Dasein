package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AccMastrLookupChkConfig extends TransformerConfig {
    @JsonProperty("GroupByFields")
    private List<String> groupByFields;

    @JsonProperty("Key")
    private String key;

    @JsonProperty("ExceedCountThreshold")
    private long exceededCountThreshold;

    @JsonProperty("CntLessThanThresholdFlag")
    private boolean cntLessThanThresholdFlag;

    @JsonProperty("ID")
    private List<String> id;

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

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
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
