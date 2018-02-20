package com.latticeengines.domain.exposed.metadata.transaction;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Product implements Serializable {

    @JsonProperty("ProductId")
    private String productId;

    @JsonProperty("ProductName")
    private String productName;

    @JsonProperty("ProductBundle")
    private String productBundle;

    @JsonProperty("ProductType")
    private ProductType type;

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

    public String getProductBundle() {
        return productBundle;
    }

    public void setProductBundle(String productBundle) {
        this.productBundle = productBundle;
    }

    public ProductType getProductType() {
        return type;
    }

    public void setProductType(ProductType type) {
        this.type = type;
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
