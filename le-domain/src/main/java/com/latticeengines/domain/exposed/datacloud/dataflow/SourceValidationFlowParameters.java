package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ValidationConfig;

public class SourceValidationFlowParameters extends TransformationFlowParameters {

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
