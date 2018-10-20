package com.latticeengines.domain.exposed.serviceapps.core;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttrConfigCategoryOverview<T extends Serializable> {

    @JsonProperty("TotalAttrs")
    private Long totalAttrs;
    @JsonProperty("Limit")
    private Long limit;
    @JsonProperty("PropSummary")
    private Map<String, Map<T, Long>> propSummary;

    public Long getTotalAttrs() {
        return totalAttrs;
    }

    public void setTotalAttrs(Long totalAttrsVal) {
        totalAttrs = totalAttrsVal;
    }

    public Long getLimit() {
        return limit;
    }

    public void setLimit(Long limitVal) {
        limit = limitVal;
    }

    public Map<String, Map<T, Long>> getPropSummary() {
        return propSummary;
    }

    public void setPropSummary(Map<String, Map<T, Long>> propSummaryVal) {
        propSummary = propSummaryVal;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
