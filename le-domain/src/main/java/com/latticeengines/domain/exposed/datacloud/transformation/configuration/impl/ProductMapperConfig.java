package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.latticeengines.domain.exposed.metadata.transaction.Product;

public class ProductMapperConfig extends TransformerConfig {

    @JsonProperty("ProductField")
    private String productField;
    @JsonProperty("ProductMap")
    private Map<String, Product> productMap;

    public String getProductField() {
        return this.productField;
    }

    public void setProductField(String productField) {
        this.productField = productField;
    }

    public Map<String, Product> getProductMap() {
        return this.productMap;
    }

    public void setProductMap(Map<String, Product> productMap) {
        this.productMap = productMap;
    }
}
