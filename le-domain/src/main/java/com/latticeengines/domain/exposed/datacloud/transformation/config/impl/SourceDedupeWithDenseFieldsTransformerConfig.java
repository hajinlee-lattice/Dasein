package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceDedupeWithDenseFieldsTransformerConfig extends TransformerConfig {

    @JsonProperty("DedupeFields")
    private List<String> dedupeFields;

    @JsonProperty("DenseFields")
    private List<String> denseFields;

    @JsonProperty("SortFields")
    private List<String> sortFields;

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

    public List<String> getSortFields() {
        return sortFields;
    }

    public void setSortFields(List<String> sortFields) {
        this.sortFields = sortFields;
    }

}
