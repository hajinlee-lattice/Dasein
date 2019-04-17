package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SampleTransformerConfig extends TransformerConfig {

    @JsonProperty("Fraction")
    private Float fraction;

    @JsonProperty("Filter")
    private String filter;

    @JsonProperty("ReportAttrs")
    private List<String> reportAttrs;

    @JsonProperty("ExcludeAttrs")
    private List<String> excludeAttrs;

    @JsonProperty("FilterFields")
    private List<String> filterFields;

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

    public List<String> getReportAttrs() {
        return reportAttrs;
    }

    public void setReportAttrs(List<String> reportAttrs) {
        this.reportAttrs = reportAttrs;
    }

    public List<String> getExcludeAttrs() {
        return excludeAttrs;
    }

    public void setExcludeAttrs(List<String> excludeAttrs) {
        this.excludeAttrs = excludeAttrs;
    }

    public List<String> getFilterFields() {
        return this.filterFields;
    }

    public void setFilterFields(List<String> filterFields) {
        this.filterFields = filterFields;
    }

}
