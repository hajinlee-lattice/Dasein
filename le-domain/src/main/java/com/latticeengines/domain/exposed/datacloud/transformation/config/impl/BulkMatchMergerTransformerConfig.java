package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BulkMatchMergerTransformerConfig extends TransformerConfig {

    @JsonProperty("joinField")
    private String joinField;

    @JsonProperty("reverse")
    private boolean reverse;

    public String getJoinField() {
        return joinField;
    }

    public void setJoinField(String joinField) {
        this.joinField = joinField;
    }

    public boolean isReverse() {
        return reverse;
    }

    public void setReverse(boolean reverse) {
        this.reverse = reverse;
    }

}
