package com.latticeengines.domain.exposed.datacloud.statistics;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Category;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TopNAttributeTree {

    @JsonProperty("Categories")
    private HashMap<String, TopNAttributes> categories;

    public void put(Category category, TopNAttributes attributes) {
        if (categories == null) {
            categories = new HashMap<>();
        }
        categories.put(category.getName(), attributes);
    }

    public TopNAttributes get(Category category) {
        if (categories == null) {
            return null;
        } else {
            return categories.get(category.getName());
        }
    }

    private HashMap<String, TopNAttributes> getCategories() {
        return categories;
    }

    private void setCategories(HashMap<String, TopNAttributes> categories) {
        this.categories = categories;
    }
}
