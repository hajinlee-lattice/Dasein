package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PeriodDataDistributorConfig extends TransformerConfig {

    @JsonProperty("PeriodField")
    private String periodField;

    @JsonProperty("PeriodIdx")
    private Integer periodIdx;

    @JsonProperty("InputIdx")
    private Integer inputIdx;

    @JsonProperty("TransactionIdx")
    private Integer transactinIdx;

    public String getPeriodField() {
        return periodField;
    }

    public void setPeriodField(String periodField) {
        this.periodField = periodField;
    }

    public Integer getPeriodIdx() {
        return periodIdx;
    }

    public void setPeriodIdx(Integer periodIdx) {
        this.periodIdx = periodIdx;
    }

    public Integer getInputIdx() {
        return inputIdx;
    }

    public void setInputIdx(Integer inputIdx) {
        this.inputIdx = inputIdx;
    }

    public Integer getTransactinIdx() {
        return transactinIdx;
    }

    public void setTransactinIdx(Integer transactinIdx) {
        this.transactinIdx = transactinIdx;
    }
}
