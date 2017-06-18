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
    @JsonProperty("Count")
    private Long count = 0L;

    @JsonProperty("Subcategories")
    private Map<String, SubcategoryStatistics> subcategories = new HashMap<>();

    public Map<String, SubcategoryStatistics> getSubcategories() {
        return subcategories;
    }

    public void setSubcategories(Map<String, SubcategoryStatistics> subcategories) {
        this.subcategories = subcategories;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    void updateCount() {
        subcategories.values().forEach(SubcategoryStatistics::updateCount);
        count = subcategories.values().stream().mapToLong(SubcategoryStatistics::getCount).sum();
    }
}
