package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BucketedFilterConfig extends TransformerConfig {

    @JsonProperty("OriginalAttrs")
    private List<String> originalAttrs;

    public List<String> getOriginalAttrs() {
        return originalAttrs;
    }

    public void setOriginalAttrs(List<String> originalAttrs) {
        this.originalAttrs = originalAttrs;
    }
}
