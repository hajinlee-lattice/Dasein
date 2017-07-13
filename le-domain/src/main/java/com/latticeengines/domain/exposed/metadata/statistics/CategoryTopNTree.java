package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CategoryTopNTree {

    @JsonProperty("Subcategories")
    private Map<String, List<TopAttribute>> subcategories = new HashMap<>();

    public Map<String, List<TopAttribute>> getSubcategories() {
        return subcategories;
    }

    public void setSubcategories(Map<String, List<TopAttribute>> subcategories) {
        this.subcategories = subcategories;
    }
}
