package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Statistics {
    @JsonProperty("Categories")
    private Map<String, CategoryStatistics> categories = new HashMap<>();

    public Map<String, CategoryStatistics> getCategories() {
        return categories;
    }

    public void setCategories(Map<String, CategoryStatistics> categories) {
        this.categories = categories;
    }
}
