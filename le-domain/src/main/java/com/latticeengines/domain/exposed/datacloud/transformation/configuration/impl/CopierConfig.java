package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CopierConfig extends TransformerConfig {

    @JsonProperty("RetainAttrs")
    private List<String> retainAttrs;

    public List<String> getRetainAttrs() {
        return retainAttrs;
    }

    public void setRetainAttrs(List<String> retainAttrs) {
        this.retainAttrs = retainAttrs;
    }
}
