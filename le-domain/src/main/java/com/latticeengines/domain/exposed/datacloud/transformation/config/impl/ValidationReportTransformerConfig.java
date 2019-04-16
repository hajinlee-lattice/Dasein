package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ValidationReportTransformerConfig extends TransformerConfig {

    @JsonProperty("Rules")
    private List<ValidationConfig> rules;

    @JsonProperty("ReportAttrs")
    private List<String> reportAttrs;

    public List<ValidationConfig> getRules() {
        return rules;
    }

    public void setRules(List<ValidationConfig> rules) {
        this.rules = rules;
    }

    public List<String> getReportAttrs() {
        return reportAttrs;
    }

    public void setReportAttrs(List<String> reportAttrs) {
        this.reportAttrs = reportAttrs;
    }
}
