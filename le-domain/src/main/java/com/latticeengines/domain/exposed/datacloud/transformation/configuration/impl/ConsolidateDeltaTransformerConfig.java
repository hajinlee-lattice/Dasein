package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

public class ConsolidateDeltaTransformerConfig extends TransformerConfig {

    private String srcIdField;

    public String getSrcIdField() {
        return srcIdField;
    }

    public void setSrcIdField(String srcIdField) {
        this.srcIdField = srcIdField;
    }

}
