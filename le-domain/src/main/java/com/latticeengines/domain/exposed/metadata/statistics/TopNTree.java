package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.latticeengines.domain.exposed.metadata.Category;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TopNTree {

    @JsonProperty("Categories")
    @JsonDeserialize(keyUsing = Category.CategoryKeyDeserializer.class)
    @JsonSerialize(keyUsing = Category.CategoryKeySerializer.class)
    private Map<Category, CategoryTopNTree> categories = new HashMap<>();

    public Map<Category, CategoryTopNTree> getCategories() {
        if (!(this.categories instanceof TreeMap)) {
            TreeMap<Category, CategoryTopNTree> treeMap = new TreeMap<>(Comparator.comparing(Category::getOrder));
            treeMap.putAll(this.categories);
            this.categories = treeMap;
        }
        return categories;
    }

    public void setCategories(Map<Category, CategoryTopNTree> categories) {
        if (categories instanceof TreeMap) {
            this.categories = categories;
        } else {
            this.categories = new TreeMap<>(Comparator.comparing(Category::getOrder));
            this.categories.putAll(categories);
        }
    }

    public boolean hasCategory(Category category) {
        return categories.containsKey(category);
    }

    public CategoryTopNTree getCategory(Category category) {
        return categories.get(category);
    }

    public void putCategory(Category category, CategoryTopNTree categoryTopNTree) {
        categories.put(category, categoryTopNTree);
    }
}
