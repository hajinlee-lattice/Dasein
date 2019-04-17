package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

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
    @JsonProperty("keepInternalColumns")
    private boolean keepInternalColumns = true;

    @JsonProperty("GroupFields")
    private List<String> groupFields;

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

    public boolean isKeepInternalColumns() {
        return keepInternalColumns;
    }

    public void setKeepInternalColumns(boolean keepInternalColumns) {
        this.keepInternalColumns = keepInternalColumns;
    }

    public List<String> getGroupFields() {
        return groupFields;
    }

    public void setGroupFields(List<String> groupFields) {
        this.groupFields = groupFields;
    }

}
