package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MergeProductReport {

    @JsonProperty("Records")
    private long records;

    @JsonProperty("InvalidRecords")
    private long invalidRecords;

    @JsonProperty("BundleProducts")
    private long bundleProducts;

    @JsonProperty("HierarchyProducts")
    private long hierarchyProducts;

    @JsonProperty("AnalyticProducts")
    private long analyticProducts;

    @JsonProperty("SpendingProducts")
    private long spendingProducts;

    @JsonProperty("Errors")
    private List<String> errors;

    public long getRecords() {
        return records;
    }

    public void setRecords(long records) {
        this.records = records;
    }

    public long getInvalidRecords() {
        return invalidRecords;
    }

    public void setInvalidRecords(long invalidRecords) {
        this.invalidRecords = invalidRecords;
    }

    public long getBundleProducts() {
        return bundleProducts;
    }

    public void setBundleProducts(long bundleProducts) {
        this.bundleProducts = bundleProducts;
    }

    public long getHierarchyProducts() {
        return hierarchyProducts;
    }

    public void setHierarchyProducts(long hierarchyProducts) {
        this.hierarchyProducts = hierarchyProducts;
    }

    public long getAnalyticProducts() {
        return analyticProducts;
    }

    public void setAnalyticProducts(long analyticProducts) {
        this.analyticProducts = analyticProducts;
    }

    public long getSpendingProducts() {
        return spendingProducts;
    }

    public void setSpendingProducts(long spendingProducts) {
        this.spendingProducts = spendingProducts;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setErrors(List<String> errors) {
        this.errors = errors;
    }
}
