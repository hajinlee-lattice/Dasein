package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AccMastrIdChkConfig extends TransformerConfig {
    @JsonProperty("Status")
    private String status;

    @JsonProperty("Domain")
    private String domain;

    @JsonProperty("Duns")
    private String duns;

    @JsonProperty("Key")
    private String key;

    @JsonProperty("GroupByFields")
    private List<String> groupByFields;

    @JsonProperty("RedirectFromId")
    private String redirectFromId;

    @JsonProperty("checkDupWithStatus")
    private Boolean checkDupWithStatus;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Boolean getCheckDupWithStatus() {
        return checkDupWithStatus;
    }

    public void setCheckDupWithStatus(Boolean checkDupWithStatus) {
        this.checkDupWithStatus = checkDupWithStatus;
    }

    public String getRedirectFromId() {
        return redirectFromId;
    }

    public void setRedirectFromId(String redirectFromId) {
        this.redirectFromId = redirectFromId;
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

    public String getKey() {
        return duns;
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
}
