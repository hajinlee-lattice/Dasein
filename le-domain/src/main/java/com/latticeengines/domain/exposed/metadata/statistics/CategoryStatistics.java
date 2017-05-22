package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CategoryStatistics {
    @JsonProperty("Subcategories")
    private Map<String, SubcategoryStatistics> subcategories = new HashMap<>();

    public Map<String, SubcategoryStatistics> getSubcategories() {
        return subcategories;
    }

    public void setSubcategories(Map<String, SubcategoryStatistics> subcategories) {
        this.subcategories = subcategories;
    }
}
