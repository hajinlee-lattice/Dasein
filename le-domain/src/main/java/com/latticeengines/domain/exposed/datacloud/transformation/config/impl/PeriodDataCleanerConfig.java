package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PeriodDataCleanerConfig extends TransformerConfig {

    @JsonProperty("PeriodField")
    private String periodField;

    @JsonProperty("PeriodNameField")
    private String periodNameField;

    @JsonProperty("TransactionIdxes")
    private Map<String, Integer> transactionIdxes;

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

    public boolean isMultiPeriod() {
        return multiPeriod;
    }

    public void setMultiPeriod(boolean multiPeriod) {
        this.multiPeriod = multiPeriod;
    }

    public Map<String, Integer> getTransactionIdxes() {
        return transactionIdxes;
    }

    public void setTransactionIdxes(Map<String, Integer> transactionIdxes) {
        this.transactionIdxes = transactionIdxes;
    }

}
