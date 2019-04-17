package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BucketedFilterConfig extends TransformerConfig {

    @JsonProperty("OriginalAttrs")
    private List<String> originalAttrs;

    @JsonProperty("EncAttrPrefix")
    private String encAttrPrefix;

    public List<String> getOriginalAttrs() {
        return originalAttrs;
    }

    public void setOriginalAttrs(List<String> originalAttrs) {
        this.originalAttrs = originalAttrs;
    }

    public String getEncAttrPrefix() {
        return encAttrPrefix;
    }

    public void setEncAttrPrefix(String encAttrPrefix) {
        this.encAttrPrefix = encAttrPrefix;
    }
}
