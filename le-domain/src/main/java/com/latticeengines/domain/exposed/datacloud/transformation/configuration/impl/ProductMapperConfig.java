package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProductMapperConfig extends TransformerConfig {

    @JsonProperty("ProductField")
    private String productField;

    @JsonProperty("ProductTypeField")
    private String productType;

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

}
