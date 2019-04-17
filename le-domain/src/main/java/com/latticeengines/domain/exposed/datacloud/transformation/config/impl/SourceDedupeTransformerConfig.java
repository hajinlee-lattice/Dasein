package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceDedupeTransformerConfig extends TransformerConfig {

    @JsonProperty("DedupeField")
    private String dedupeField;

    public String getDedupeField() {
        return dedupeField;
    }

    public void setDedupeField(String dedupeField) {
        this.dedupeField = dedupeField;
    }
}
