package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SampleTransformerConfig extends TransformerConfig {

    @JsonProperty("Fraction")
    private Float fraction;

    @JsonProperty("Filter")
    private String filter;

    @JsonProperty("FilterAttrs")
    private List<String> filterAttrs;

    public Float getFraction() {
        return fraction;
    }

    public void setFraction(Float fraction) {
        this.fraction = fraction;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public List<String> getFilterAttrs() {
        return filterAttrs;
    }

    public void setFilterAttrs(List<String> filterAttrs) {
        this.filterAttrs = filterAttrs;
    }
}
