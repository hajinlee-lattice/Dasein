package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CategorizedIntervalBucket extends IntervalBucket {
    private static final long serialVersionUID = 7888809618514182701L;

    @Override
    @JsonIgnore
    public String getAlgorithm() {
        return CATEGORIZED_INTERVAL;
    }

    @JsonProperty("cats")
    private List<String> categories;

    public List<String> getCategories() {
        return categories;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    @Override
    @JsonIgnore
    public List<String> generateLabelsInternal() {
        List<String> labels = new ArrayList<>();
        labels.add(null);
        labels.addAll(categories);
        return labels;
    }
}
