package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CategoryStatistics {

    @JsonProperty("Subcategories")
    private Map<String, SubcategoryStatistics> subcategories = new HashMap<>();

    public Map<String, SubcategoryStatistics> getSubcategories() {
        return subcategories;
    }

    public void setSubcategories(Map<String, SubcategoryStatistics> subcategories) {
        this.subcategories = subcategories;
    }

    public SubcategoryStatistics getSubcategory(String subcategory) {
        return subcategories.get(subcategory);
    }

    public void putSubcategory(String subcategory, SubcategoryStatistics subcatStats) {
        subcategories.put(subcategory, subcatStats);
    }

    public boolean hasSubcategory(String subcategory) {
        return subcategories.containsKey(subcategory);
    }
}
