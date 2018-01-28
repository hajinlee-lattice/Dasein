package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class Statistics {

    @JsonProperty("Counts")
    private Map<BusinessEntity, Long> counts;

    @JsonProperty("Categories")
    @JsonDeserialize(keyUsing = Category.CategoryKeyDeserializer.class)
    @JsonSerialize(keyUsing = Category.CategoryKeySerializer.class)
    private Map<Category, CategoryStatistics> categories = new HashMap<>();

    public Map<Category, CategoryStatistics> getCategories() {
        return categories;
    }

    public void setCategories(Map<Category, CategoryStatistics> categories) {
        this.categories = categories;
    }

    public Map<BusinessEntity, Long> getCounts() {
        return counts;
    }

    public void setCounts(Map<BusinessEntity, Long> counts) {
        this.counts = counts;
    }

    public CategoryStatistics getCategory(Category category) {
        return categories.get(category);
    }

    public void putCategory(Category category, CategoryStatistics cateStats) {
        categories.put(category, cateStats);
    }

    public boolean hasCategory(Category category) {
        return categories.containsKey(category);
    }

}
