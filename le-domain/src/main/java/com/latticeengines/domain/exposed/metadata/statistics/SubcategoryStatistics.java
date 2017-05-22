package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.ColumnLookup;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SubcategoryStatistics {
    @JsonProperty("Attributes")
    private Map<ColumnLookup, AttributeStatistics> attributes = new HashMap<>();

    public Map<ColumnLookup, AttributeStatistics> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<ColumnLookup, AttributeStatistics> attributes) {
        this.attributes = attributes;
    }
}
