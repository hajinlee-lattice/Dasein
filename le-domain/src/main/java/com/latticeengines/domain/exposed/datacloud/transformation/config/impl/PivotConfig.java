package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

public class PivotConfig extends TransformerConfig {
    private String[] joinFields;
    private Boolean hasSqlPresence = true;

    public String[] getJoinFields() {
        return joinFields;
    }

    public void setJoinFields(String[] joinFields) {
        this.joinFields = joinFields;
    }

    public Boolean hasSqlPresence() {
        return hasSqlPresence;
    }

    public void setHasSqlPresence(Boolean hasSqlPresence) {
        this.hasSqlPresence = hasSqlPresence;
    }
}
