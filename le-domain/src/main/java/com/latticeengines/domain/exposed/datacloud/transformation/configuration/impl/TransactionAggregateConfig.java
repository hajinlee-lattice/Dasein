package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.Map;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.latticeengines.domain.exposed.metadata.transaction.Product;

public class TransactionAggregateConfig extends TransformerConfig {

    @JsonProperty("ProductMap")
    private Map<String, Product> productMap;

    @JsonProperty("TransactionType")
    private String transactionType;

    @JsonProperty("IdField")
    private String idField;

    @JsonProperty("AccountField")
    private String accountField;

    @JsonProperty("ProductField")
    private String productField;

    @JsonProperty("DateField")
    private String dateField;

    @JsonProperty("QuantityField")
    private String quantityField;

    @JsonProperty("AmountField")
    private String amountField;

    @JsonProperty("TypeField")
    private String typeField;

    @JsonProperty("Periods")
    private List<String> periods;

    @JsonProperty("Metrics")
    private List<String> metrics;

    public Map<String, Product> getProductMap() {
        return productMap;
    }

    public void setProductMap(Map<String, Product> productMap) {
        this.productMap = productMap;
    }

    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public void setIdField(String idField) {
        this.idField = idField;
    }

    public String getIdField() {
        return idField;
    }

    public void setAccountField(String accountField) {
        this.accountField = accountField;
    }

    public String getAccountField() {
        return accountField;
    }

    public void setProductField(String productField) {
        this.productField = productField;
    }

    public String getProductField() {
        return productField;
    }

    public void setDateField(String dateField) {
        this.dateField = dateField;
    }

    public String getDateField() {
        return dateField;
    }

    public void setQuantityField(String quantityField) {
        this.quantityField = quantityField;
    }

    public String getQuantityField() {
        return quantityField;
    }

    public void setAmountField(String amountField) {
        this.amountField = amountField;
    }

    public String getAmountField() {
        return amountField;
    }

    public void setTypeField(String typeField) {
        this.typeField = typeField;
    }

    public String getTypeField() {
        return typeField;
    }

    public void setPeriods(List<String> periods) {
        this.periods = periods;
    }

    public List<String> getPeriods() {
        return periods;
    }

    public void setMetrics(List<String> metrics) {
        this.metrics = metrics;
    }

    public List<String> getMetrics() {
        return metrics;
    }

}
