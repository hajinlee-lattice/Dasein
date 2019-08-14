package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PeriodDataDistributorConfig extends TransformerConfig {

    @JsonProperty("PeriodField")
    private String periodField;

    @JsonProperty("PeriodNameField")
    private String periodNameField;

    @JsonProperty("PeriodIdx")
    private Integer periodIdx;

    @JsonProperty("InputIdx")
    private Integer inputIdx;

    @JsonProperty("TransactionIdx")
    private Integer transactinIdx;

    @JsonProperty("MultiPeriod")
    private boolean multiPeriod;

    @JsonProperty("TransactionIdxes")
    private Map<String, Integer> transactionIdxes; // PeriodName ->
                                                   // TransactionIdx

    // Whether to retry distribution of failed periods -- will cleanup impacted
    // period file in target dir before start/re-start distributing
    @JsonProperty("Retryable")
    private boolean retryable;

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

    public boolean isRetryable() {
        return retryable;
    }

    public void setRetryable(boolean retryable) {
        this.retryable = retryable;
    }
}
