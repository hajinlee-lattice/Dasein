package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.FilterOptions;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CategoryTopNTree {
    // show subcategory instead of attr name in category tile
    @JsonProperty("ShowSubCategory")
    private Boolean shouldShowSubCategoryInCategoryTile;

    @JsonProperty("AltName")
    private String secondaryCategoryDisplayName;

    // options for attribute filtering in this category
    @JsonProperty("FilterOptions")
    private FilterOptions filterOptions;

    @JsonProperty("Subcategories")
    private Map<String, List<TopAttribute>> subcategories = new HashMap<>();

    public Boolean getShouldShowSubCategoryInCategoryTile() {
        return shouldShowSubCategoryInCategoryTile;
    }

    public void setShouldShowSubCategoryInCategoryTile(Boolean shouldShowSubCategoryInCategoryTile) {
        this.shouldShowSubCategoryInCategoryTile = shouldShowSubCategoryInCategoryTile;
    }

    public String getSecondaryCategoryDisplayName() {
        return secondaryCategoryDisplayName;
    }

    public void setSecondaryCategoryDisplayName(String secondaryCategoryDisplayName) {
        this.secondaryCategoryDisplayName = secondaryCategoryDisplayName;
    }

    public FilterOptions getFilterOptions() {
        return filterOptions;
    }

    public void setFilterOptions(FilterOptions filterOptions) {
        this.filterOptions = filterOptions;
    }

    public Map<String, List<TopAttribute>> getSubcategories() {
        return subcategories;
    }

    public void setSubcategories(Map<String, List<TopAttribute>> subcategories) {
        this.subcategories = subcategories;
    }

    public boolean hasSubcategory(String subcategory) {
        return subcategories.containsKey(subcategory);
    }

    public List<TopAttribute> getSubcategory(String subcategory) {
        return subcategories.get(subcategory);
    }

    public void putSubcategory(String subcategory, List<TopAttribute> topAttributes) {
        subcategories.put(subcategory, topAttributes);
    }
}
