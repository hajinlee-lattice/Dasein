package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Attribute;

public class CloneModelingParameters {
    @JsonProperty
    private String sourceModelSummaryId;

    @JsonProperty
    private String name;

    @JsonProperty
    private String description;

    @JsonProperty
    private List<Attribute> attributes;

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

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<Attribute> attributes) {
        this.attributes = attributes;
    }

    public String getSourceModelSummaryId() {
        return sourceModelSummaryId;
    }

    public void setSourceModelSummaryId(String sourceModelSummaryId) {
        this.sourceModelSummaryId = sourceModelSummaryId;
    }
}
