package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JoinConfig extends TransformerConfig {
    @JsonProperty("JoinFields")
    private String[][] joinFields;

    @JsonProperty("JoinType")
    private JoinType joinType;

    public String[][] getJoinFields() {
        return joinFields;
    }

    public void setJoinFields(String[][] joinFields) {
        this.joinFields = joinFields;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public enum JoinType {
        INNER, //
        LEFT, //
        RIGHT, //
        OUTER
    }
}
