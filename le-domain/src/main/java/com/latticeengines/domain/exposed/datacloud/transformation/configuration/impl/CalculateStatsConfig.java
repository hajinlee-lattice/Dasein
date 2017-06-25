package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CalculateStatsConfig extends TransformerConfig {

    @JsonProperty("DimensionGraph")
    private Map<String, List<String>> dimensionGraph;

    @JsonProperty("DedupFields")
    private List<String> dedupFields;

    public Map<String, List<String>> getDimensionGraph() {
        return dimensionGraph;
    }

    public void setDimensionGraph(Map<String, List<String>> dimensionGraph) {
        this.dimensionGraph = dimensionGraph;
    }

    public List<String> getDedupFields() {
        return dedupFields;
    }

    public void setDedupFields(List<String> dedupFields) {
        this.dedupFields = dedupFields;
    }
}
