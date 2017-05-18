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

    @Override
    @JsonIgnore
    public String getAlgorithm() {
        return CATEGORICAL;
    }

    @JsonProperty("categories")
    private List<String> categories;

    @JsonProperty("mapping")
    private Map<String, List<String>> mapping;

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

    @Override
    @JsonIgnore
    public List<String> generateLabelsInternal () {
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
