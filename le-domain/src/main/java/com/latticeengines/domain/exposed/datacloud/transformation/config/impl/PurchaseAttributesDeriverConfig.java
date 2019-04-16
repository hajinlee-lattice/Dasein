package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class PurchaseAttributesDeriverConfig extends TransformerConfig {
    @JsonProperty("Fields")
    private List<InterfaceName> fields;

    public List<InterfaceName> getFields() {
        return fields;
    }

    public void setFields(List<InterfaceName> fields) {
        this.fields = fields;
    }
}
