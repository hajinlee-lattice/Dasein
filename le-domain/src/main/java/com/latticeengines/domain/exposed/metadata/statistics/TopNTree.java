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

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TopNTree {

    @JsonProperty("Categories")
    @JsonDeserialize(keyUsing = Category.CategoryKeyDeserializer.class)
    @JsonSerialize(keyUsing = Category.CategoryKeySerializer.class)
    private Map<Category, CategoryTopNTree> categories = new HashMap<>();

    public Map<Category, CategoryTopNTree> getCategories() {
        return categories;
    }

    public void setCategories(Map<Category, CategoryTopNTree> categories) {
        this.categories = categories;
    }
}
