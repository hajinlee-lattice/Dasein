package com.latticeengines.domain.exposed.datacloud.transformation.config.atlas;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

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
