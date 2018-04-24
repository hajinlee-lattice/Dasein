package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Category;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttrConfigActivationOverview {

    @JsonProperty("Category")
    private Category category;
    @JsonProperty("Selected")
    private Long selected;
    @JsonProperty("TotalAttrs")
    private Long totalAttrs;
    @JsonProperty("Limit")
    private Long limit;

    public void setCategory(Category category) {
        this.category = category;
    }

    public Category getCategory() {
        return this.category;
    }

    public void setSelected(Long selected) {
        this.selected = selected;
    }

    public Long getSelected() {
        return this.selected;
    }

    public void setTotalAttrs(Long totalAttrs) {
        this.totalAttrs = totalAttrs;
    }

    public Long getTotalAttrs() {
        return this.totalAttrs;
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public Long getLimit() {
        return this.limit;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
