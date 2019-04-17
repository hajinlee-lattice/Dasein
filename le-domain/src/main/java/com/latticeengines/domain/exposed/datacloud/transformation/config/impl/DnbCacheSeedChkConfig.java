package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DnbCacheSeedChkConfig extends TransformerConfig {
    @JsonProperty("Domain")
    private String domain;

    @JsonProperty("DUNS")
    private String duns;

    @JsonProperty("ExceedCountThreshold")
    private long exceededCountThreshold;

    @JsonProperty("CntLessThanThresholdFlag")
    private boolean cntLessThanThresholdFlag;

    @JsonProperty("ID")
    private List<String> id;

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

}
