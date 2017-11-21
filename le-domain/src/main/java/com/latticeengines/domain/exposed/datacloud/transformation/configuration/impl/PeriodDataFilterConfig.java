package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.period.PeriodStrategy;

public class PeriodDataFilterConfig extends TransformerConfig {

    @JsonProperty("PeriodField")
    private String periodField;

    @JsonProperty("PeriodStrategy")
    private PeriodStrategy periodStrategy;

    @JsonProperty("EarliestTransactionDate")
    private String earliestTransactionDate;

    public String getPeriodField() {
        return periodField;
    }

    public void setPeriodField(String periodField) {
        this.periodField = periodField;
    }

    public PeriodStrategy getPeriodStrategy() {
        return periodStrategy;
    }

    public void setPeriodStrategy(PeriodStrategy periodStrategy) {
        this.periodStrategy = periodStrategy;
    }

    public String getEarliestTransactionDate() {
        return earliestTransactionDate;
    }

    public void setEarliestTransactionDate(String earliestTransactionDate) {
        this.earliestTransactionDate = earliestTransactionDate;
    }
}
