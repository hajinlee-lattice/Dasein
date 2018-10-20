package com.latticeengines.domain.exposed.serviceapps.core;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Category;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttrConfigOverview<T extends Serializable> {

    @JsonProperty("Category")
    private Category category;
    @JsonProperty("TotalAttrs")
    private Long totalAttrs;
    @JsonProperty("Limit")
    private Long limit;
    @JsonProperty("PropSummary")
    private Map<String, Map<T, Long>> propSummary;

    public Category getCategory() {
        return this.category;
    }

    public void setCategory(Category category) {
        this.category = category;
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

    public Map<String, Map<T, Long>> getPropSummary() {
        return this.propSummary;
    }

    public void setPropSummary(Map<String, Map<T, Long>> propSummary) {
        this.propSummary = propSummary;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
