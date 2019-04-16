package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransformerConfig {

    @JsonProperty("Transfomer")
    private String transformer = "TransformerBase";

    // If true, target schema will retain all the properties from the first base
    // schema
    @JsonProperty("InheritSchemaProp")
    private boolean shouldInheritSchemaProp;

    public String getTransformer() {
        return transformer;
    }

    public void setTransformer(String transformer) {
        this.transformer = transformer;
    }

    public boolean isShouldInheritSchemaProp() {
        return shouldInheritSchemaProp;
    }

    public void setShouldInheritSchemaProp(boolean shouldInheritSchemaProp) {
        this.shouldInheritSchemaProp = shouldInheritSchemaProp;
    }

    public boolean validate(List<String> sourceNames) {
        if ((sourceNames == null) || (sourceNames.size() == 0)) {
            return false;
        } else {
            return true;
        }
    }
}
