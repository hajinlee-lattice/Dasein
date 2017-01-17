package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceDedupeWithDenseFieldsTransformerConfig extends TransformerConfig {

    @JsonProperty("DedupeField")
    private String dedupeField;

    public String getDedupeField() {
        return dedupeField;
    }

    public void setDedupeField(String dedupeField) {
        this.dedupeField = dedupeField;
    }
    @JsonProperty("DedupeFields")
    private List<String> dedupeFields;

    @JsonProperty("DenseFields")
    private List<String> denseFields;

    public List<String> getDedupeFields() {
        return dedupeFields;
    }

    public void setDedupeFields(List<String> dedupeFields) {
        this.dedupeFields = dedupeFields;
    }

    public List<String> getDenseFields() {
        return denseFields;
    }

    public void setDenseFields(List<String> denseFields) {
        this.denseFields = denseFields;
    }
}
