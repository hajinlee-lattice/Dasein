package com.latticeengines.domain.exposed.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class DataFlowSource implements HasName {

    private String name;
    private String rawDataPath;
    private List<ExtractFilter> extractFilters;

    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("extract_filters")
    public List<ExtractFilter> getExtractFilters() {
        return extractFilters;
    }

    @JsonProperty("extract_filters")
    public void setExtractFilters(List<ExtractFilter> extractFilters) {
        this.extractFilters = extractFilters;
    }
}