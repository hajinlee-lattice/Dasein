package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataCollectionStatusDetail implements Serializable {
    private static final long serialVersionUID = -2375077678460293618L;
    @JsonProperty("MinTxnDate")
    private Integer minTxnDate = 0;
    @JsonProperty("MaxTxnDate")
    private Integer maxTxnDate = 0;
    @JsonProperty("EvaluationDate")
    private String evaluationDate = "not set";
    @JsonProperty("DataCloudBuildNumber")
    private String dataCloudBuildNumber = "not set";
    @JsonProperty("AccountCount")
    private Long accountCount = 0L;
    @JsonProperty("ContactCount")
    private Long contactCount = 0L;
    @JsonProperty("TransactionCount")
    private Long transactionCount = 0L;

    public Integer getMinTxnDate() {
        return minTxnDate;
    }

    public void setMinTxnDate(Integer minTxnDate) {
        this.minTxnDate = minTxnDate;
    }

    public Integer getMaxTxnDate() {
        return maxTxnDate;
    }

    public void setMaxTxnDate(Integer maxTxnDate) {
        this.maxTxnDate = maxTxnDate;
    }

    public String getEvaluationDate() {
        return evaluationDate;
    }

    public void setEvaluationDate(String evaluationDate) {
        this.evaluationDate = evaluationDate;
    }

    public String getDataCloudBuildNumber() {
        return dataCloudBuildNumber;
    }

    public void setDataCloudBuildNumber(String dataCloudBuildNumber) {
        this.dataCloudBuildNumber = dataCloudBuildNumber;
    }

    public Long getAccountCount() {
        return accountCount;
    }

    public void setAccountCount(Long accountCount) {
        this.accountCount = accountCount;
    }

    public Long getContactCount() {
        return contactCount;
    }

    public void setContactCount(Long contactCount) {
        this.contactCount = contactCount;
    }

    public Long getTransactionCount() {
        return transactionCount;
    }

    public void setTransactionCount(Long transactionCount) {
        this.transactionCount = transactionCount;
    }



}
