package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CloneModelingParameters {
    @JsonProperty
    private String sourceModelSummaryId;

    @JsonProperty
    private String name;

    @JsonProperty
    private String description;

    @JsonProperty
    private List<VdbMetadataField> attributes;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<VdbMetadataField> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<VdbMetadataField> attributes) {
        this.attributes = attributes;
    }

    public String getSourceModelSummaryId() {
        return sourceModelSummaryId;
    }

    public void setSourceModelSummaryId(String sourceModelSummaryId) {
        this.sourceModelSummaryId = sourceModelSummaryId;
    }
}
