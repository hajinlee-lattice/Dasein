package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceFieldEnrichmentTransformerConfig extends TransformerConfig {

    @JsonProperty("FromFields")
    private List<String> fromFields;
    
    @JsonProperty("ToFields")
    private List<String> toFields;

    @JsonProperty("IsDebug")
    boolean isDebug;

    public List<String> getFromFields() {
        return fromFields;
    }

    public void setFromFields(List<String> fromFields) {
        this.fromFields = fromFields;
    }

    public List<String> getToFields() {
        return toFields;
    }

    public void setToFields(List<String> toFields) {
        this.toFields = toFields;
    }

    public boolean isDebug() {
        return isDebug;
    }

    public void setDebug(boolean isDebug) {
        this.isDebug = isDebug;
    }
    
    
}
