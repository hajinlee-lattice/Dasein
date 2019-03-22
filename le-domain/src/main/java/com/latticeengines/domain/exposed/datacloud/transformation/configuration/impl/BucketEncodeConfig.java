package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BucketEncodeConfig extends TransformerConfig {

    // an optional field used to do a final filter on not-null row id
    @JsonProperty("RowId")
    private String rowId;

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }
}
