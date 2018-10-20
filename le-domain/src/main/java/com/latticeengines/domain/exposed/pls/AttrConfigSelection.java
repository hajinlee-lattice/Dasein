package com.latticeengines.domain.exposed.pls;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttrConfigSelection {

    @JsonProperty("DisplayName")
    private String displayName;
    @JsonProperty("Selected")
    private Long selected;
    @JsonProperty("TotalAttrs")
    private Long totalAttrs;
    @JsonProperty("Limit")
    private Long limit;
    @JsonProperty("Categories")
    private Map<String, Long> categories;

    public String getDisplayName() {
        return this.displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public Long getSelected() {
        return this.selected;
    }

    public void setSelected(Long selected) {
        this.selected = selected;
    }

    public Long getTotalAttrs() {
        return this.totalAttrs;
    }

    public void setTotalAttrs(Long totalAttrs) {
        this.totalAttrs = totalAttrs;
    }

    public Long getLimit() {
        return this.limit;
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public Map<String, Long> getCategories() {
        return this.categories;
    }

    public void setCategories(Map<String, Long> categories) {
        this.categories = categories;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
