package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;

public class PeriodDataFilterConfig extends TransformerConfig {

    @JsonProperty("PeriodField")
    private String periodField;

    @JsonProperty("PeriodNameField")
    private String periodNameField;

    @JsonProperty("PeriodStrategies")
    private List<PeriodStrategy> periodStrategies;

    @JsonProperty("EarliestTransactionDate")
    private String earliestTransactionDate;

    @JsonProperty("MultiPeriod")
    private boolean multiPeriod;

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

    public List<PeriodStrategy> getPeriodStrategies() {
        return periodStrategies;
    }

    public void setPeriodStrategies(List<PeriodStrategy> periodStrategies) {
        this.periodStrategies = periodStrategies;
    }

    public String getEarliestTransactionDate() {
        return earliestTransactionDate;
    }

    public void setEarliestTransactionDate(String earliestTransactionDate) {
        this.earliestTransactionDate = earliestTransactionDate;
    }

    public boolean isMultiPeriod() {
        return multiPeriod;
    }

    public void setMultiPeriod(boolean multiPeriod) {
        this.multiPeriod = multiPeriod;
    }
}
