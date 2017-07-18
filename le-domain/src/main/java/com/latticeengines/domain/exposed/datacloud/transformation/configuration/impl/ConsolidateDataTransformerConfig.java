package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;


public class ConsolidateDataTransformerConfig extends TransformerConfig {

    private String srcIdField;
    private String masterIdField;

    public String getSrcIdField() {
        return srcIdField;
    }

    public void setSrcIdField(String srcIdField) {
        this.srcIdField = srcIdField;
    }

    public String getMasterIdField() {
        return masterIdField;
    }

    public void setMasterIdField(String masterIdField) {
        this.masterIdField = masterIdField;
    }
}
