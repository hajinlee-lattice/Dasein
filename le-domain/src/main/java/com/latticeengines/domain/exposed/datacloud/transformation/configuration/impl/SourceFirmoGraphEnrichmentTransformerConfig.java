package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceFirmoGraphEnrichmentTransformerConfig extends TransformerConfig {

    @JsonProperty("LeftMatchField")
    private String leftMatchField;
    @JsonProperty("RightMatchField")
    private String rightMatchField;
    @JsonProperty("EnrichingFields")
    private List<String> enrichingFields;
    @JsonProperty("EnrichedFields")
    private List<String> enrichedFields;
    @JsonProperty("SortFields")
    private List<String> sortFields;

    public String getLeftMatchField() {
        return leftMatchField;
    }

    public void setLeftMatchField(String leftMatchField) {
        this.leftMatchField = leftMatchField;
    }

    public String getRightMatchField() {
        return rightMatchField;
    }

    public void setRightMatchField(String rightMatchField) {
        this.rightMatchField = rightMatchField;
    }

    public List<String> getEnrichingFields() {
        return enrichingFields;
    }

    public void setEnrichingFields(List<String> enrichingFields) {
        this.enrichingFields = enrichingFields;
    }

    public List<String> getEnrichedFields() {
        return enrichedFields;
    }

    public void setEnrichedFields(List<String> enrichedFields) {
        this.enrichedFields = enrichedFields;
    }

    public List<String> getSortFields() {
        return this.sortFields;
    }

    public void setSortFields(List<String> sortFields) {
        this.sortFields = sortFields;
    }

}
