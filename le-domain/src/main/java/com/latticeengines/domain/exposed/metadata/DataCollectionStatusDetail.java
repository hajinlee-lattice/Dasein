package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataCollectionStatusDetail implements Serializable {
    private static final long serialVersionUID = 7817179046757931427L;

    public static final String NOT_SET = "not set";

    @JsonProperty("DateMap")
    private Map<String, Long> dateMap;

    // catalogName -> original file name used to build catalog store
    @JsonProperty("OrigCatalogFileMap")
    private Map<String, String> origCatalogFileMap;

    @JsonProperty("MinTxnDate")
    private Integer minTxnDate = 0;

    @JsonProperty("MaxTxnDate")
    private Integer maxTxnDate = 0;

    @JsonProperty("EvaluationDate")
    private String evaluationDate = NOT_SET;

    @JsonProperty("DataCloudBuildNumber")
    private String dataCloudBuildNumber = NOT_SET;

    @JsonProperty("AccountCount")
    private Long accountCount = 0L;

    @JsonProperty("ContactCount")
    private Long contactCount = 0L;

    @JsonProperty("TransactionCount")
    private Long transactionCount = 0L;

    @JsonProperty("ProductCount")
    private Long productCount = 0L;

    @JsonProperty("OrphanContactCount")
    private Long orphanContactCount = 0L;

    @JsonProperty("OrphanTransactionCount")
    private Long orphanTransactionCount = 0L;

    @JsonProperty("UnmatchedAccountCount")
    private Long unmatchedAccountCount = 0L;

    @JsonProperty("ApsRollingPeriod")
    private String apsRollingPeriod;

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

    public Long getProductCount() {
        return productCount;
    }

    public void setProductCount(Long productCount) {
        this.productCount = productCount;
    }

    public Long getOrphanContactCount() {
        return orphanContactCount;
    }

    public void setOrphanContactCount(Long orphanContactCount) {
        this.orphanContactCount = orphanContactCount;
    }

    public Long getOrphanTransactionCount() {
        return orphanTransactionCount;
    }

    public void setOrphanTransactionCount(Long orphanTransactionCount) {
        this.orphanTransactionCount = orphanTransactionCount;
    }

    public Long getUnmatchedAccountCount() {
        return unmatchedAccountCount;
    }

    public void setUnmatchedAccountCount(Long unmatchedAccountCount) {
        this.unmatchedAccountCount = unmatchedAccountCount;
    }

    public String getApsRollingPeriod() {
        return apsRollingPeriod;
    }

    public void setApsRollingPeriod(String apsRollingPeriod) {
        this.apsRollingPeriod = apsRollingPeriod;
    }

    public Map<String, Long> getDateMap() {
        return dateMap;
    }

    public void setDateMap(Map<String, Long> dateMap) {
        this.dateMap = dateMap;
    }

    public Map<String, String> getOrigCatalogFileMap() {
        return origCatalogFileMap;
    }

    public void setOrigCatalogFileMap(Map<String, String> origCatalogFileMap) {
        this.origCatalogFileMap = origCatalogFileMap;
    }
}
