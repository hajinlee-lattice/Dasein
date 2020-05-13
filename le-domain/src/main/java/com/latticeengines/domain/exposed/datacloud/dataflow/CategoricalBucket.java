package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CategoricalBucket extends BucketAlgorithm {
    private static final long serialVersionUID = 974998427768883957L;
    @JsonProperty("cats")
    private List<String> categories;

    // optional
    @JsonProperty("map")
    private Map<String, List<String>> mapping;

    // optional
    @JsonProperty("counts")
    private List<Long> counts;

    @Override
    @JsonIgnore
    public String getAlgorithm() {
        return CATEGORICAL;
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    public Map<String, List<String>> getMapping() {
        return mapping;
    }

    public void setMapping(Map<String, List<String>> mapping) {
        this.mapping = mapping;
    }

    public List<Long> getCounts() {
        return counts;
    }

    public void setCounts(List<Long> counts) {
        this.counts = counts;
    }

    @Override
    @JsonIgnore
    public List<String> generateLabelsInternal() {
        List<String> labels = new ArrayList<>();
        labels.add(null);
        labels.addAll(categories);
        return labels;
    }

    @JsonIgnore
    @Override
    public BucketType getBucketType() {
        return BucketType.Enum;
    }
}
