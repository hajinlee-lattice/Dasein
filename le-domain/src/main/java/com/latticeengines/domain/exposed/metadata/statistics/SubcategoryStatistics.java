package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SubcategoryStatistics {
    @JsonProperty("Attributes")
    private Map<String, AttributeStatistics> attributes = new HashMap<>();

    @JsonProperty("Cnt")
    private Long nonNullCount;
}
