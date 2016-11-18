package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransformerConfig {
    @JsonProperty("Transfomer")
    private String transformer = "TransformerBase";

    public String getTransformer() {
        return transformer;
    }

    public void setTransformer(String transformer) {
        this.transformer = transformer;
    }
}
