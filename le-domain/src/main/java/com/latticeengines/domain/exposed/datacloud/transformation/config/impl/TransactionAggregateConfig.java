package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.transaction.Product;

public class TransactionAggregateConfig extends TransformerConfig {

    @JsonProperty("ProductMap")
    private Map<String, List<Product>> productMap;

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

    public Map<String, List<Product>> getProductMap() {
        return productMap;
    }

    public void setProductMap(Map<String, List<Product>> productMap) {
        this.productMap = productMap;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }

    public String getIdField() {
        return idField;
    }

    public void setIdField(String idField) {
        this.idField = idField;
    }

    public String getAccountField() {
        return accountField;
    }

    public void setAccountField(String accountField) {
        this.accountField = accountField;
    }

    public String getProductField() {
        return productField;
    }

    public void setProductField(String productField) {
        this.productField = productField;
    }

    public String getDateField() {
        return dateField;
    }

    public void setDateField(String dateField) {
        this.dateField = dateField;
    }

    public String getQuantityField() {
        return quantityField;
    }

    public void setQuantityField(String quantityField) {
        this.quantityField = quantityField;
    }

    public String getAmountField() {
        return amountField;
    }

    public void setAmountField(String amountField) {
        this.amountField = amountField;
    }

    public String getTypeField() {
        return typeField;
    }

    public void setTypeField(String typeField) {
        this.typeField = typeField;
    }

    public List<String> getPeriods() {
        return periods;
    }

    public void setPeriods(List<String> periods) {
        this.periods = periods;
    }

    public List<String> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<String> metrics) {
        this.metrics = metrics;
    }

}
