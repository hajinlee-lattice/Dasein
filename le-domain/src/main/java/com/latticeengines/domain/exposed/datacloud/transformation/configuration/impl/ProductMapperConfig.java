package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.transaction.Product;

public class ProductMapperConfig extends TransformerConfig {

    @JsonProperty("ProductField")
    private String productField;

    @JsonProperty("ProductTypeField")
    private String productType;

    @JsonProperty("ProductMap")
    private Map<String, List<Product>> productMap;

    @JsonProperty("ProductTable")
    private Table productTable;

    public String getProductField() {
        return this.productField;
    }

    public void setProductField(String productField) {
        this.productField = productField;
    }

    public String getProductTypeField() {
        return this.productType;
    }

    public void setProductTypeField(String productType) {
        this.productType = productType;
    }

    public Map<String, List<Product>> getProductMap() {
        return this.productMap;
    }

    public void setProductMap(Map<String, List<Product>> productMap) {
        this.productMap = productMap;
    }

    public Table getProductTable() {
        return productTable;
    }

    public void setProductTable(Table productTable) {
        this.productTable = productTable;
    }
}
