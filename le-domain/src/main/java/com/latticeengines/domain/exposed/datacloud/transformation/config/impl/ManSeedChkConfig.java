package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ManSeedChkConfig extends TransformerConfig {
    @JsonProperty("GroupByFields")
    private List<String> groupByFields;

    @JsonProperty("Domain")
    private String domain;

    @JsonProperty("DUNS")
    private String duns;

    @JsonProperty("ID")
    private List<String> id;

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

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }

}
