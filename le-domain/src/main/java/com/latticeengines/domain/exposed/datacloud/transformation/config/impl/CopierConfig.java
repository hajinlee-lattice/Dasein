package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CopierConfig extends TransformerConfig {

    @JsonProperty("RetainAttrs")
    private List<String> retainAttrs;

    @JsonProperty("SortKeys")
    private List<String> sortKeys;

    @JsonProperty("SortDecending")
    private Boolean sortDecending;

    public List<String> getRetainAttrs() {
        return retainAttrs;
    }

    public void setRetainAttrs(List<String> retainAttrs) {
        this.retainAttrs = retainAttrs;
    }

    public List<String> getSortKeys() {
        return sortKeys;
    }

    public void setSortKeys(List<String> sortKeys) {
        this.sortKeys = sortKeys;
    }

    public Boolean getSortDecending() {
        return sortDecending;
    }

    public void setSortDecending(Boolean sortDecending) {
        this.sortDecending = sortDecending;
    }
}
