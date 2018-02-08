package com.latticeengines.domain.exposed.metadata.transaction;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Product implements Serializable {

    @JsonProperty("ProductId")
    private String productId;

    @JsonProperty("ProductName")
    private String productName;

    @JsonProperty("BundleId")
    private String bundleId;

    @JsonProperty("ProductDescription")
    private String description;

    @JsonProperty("ProductLine")
    private String productLine;

    @JsonProperty("ProductFamily")
    private String productFamily;

    @JsonProperty("ProductCategory")
    private String productCategory;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getBundleId() {
        return bundleId;
    }

    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getProductLine() {
        return productLine;
    }

    public void setProductLine(String productLine) {
        this.productLine = productLine;
    }

    public String getProductFamily() {
        return productFamily;
    }

    public void setProductFamily(String productFamily) {
        this.productFamily = productFamily;
    }

    public String getProductCategory() {
        return productCategory;
    }

    public void setProductCategory(String productCategory) {
        this.productCategory = productCategory;
    }
}
