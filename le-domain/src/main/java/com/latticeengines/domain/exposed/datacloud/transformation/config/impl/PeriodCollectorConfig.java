package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PeriodCollectorConfig extends TransformerConfig {

    @JsonProperty("PeriodField")
    private String periodField;

    @JsonProperty("PeriodNameField")
    private String periodNameField;

    public String getPeriodField() {
        return periodField;
    }

    public void setPeriodField(String periodField) {
        this.periodField = periodField;
    }

    public String getPeriodNameField() {
        return periodNameField;
    }

    public void setPeriodNameField(String periodNameField) {
        this.periodNameField = periodNameField;
    }

}
