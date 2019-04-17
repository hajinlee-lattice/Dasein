package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

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

    @JsonProperty("DimensionTree")
    private Map<String, List<String>> dimensionTree;

    @JsonProperty("DedupFields")
    private List<String> dedupFields;

    public Map<String, List<String>> getDimensionTree() {
        return dimensionTree;
    }

    public void setDimensionTree(Map<String, List<String>> dimensionTree) {
        this.dimensionTree = dimensionTree;
    }

    public List<String> getDedupFields() {
        return dedupFields;
    }

    public void setDedupFields(List<String> dedupFields) {
        this.dedupFields = dedupFields;
    }
}
