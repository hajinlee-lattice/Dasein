package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TrimConfig extends TransformerConfig {

    @JsonProperty("trim_columns")
    private List<String> trimColumns;

    public List<String> getTrimColumns() {
        return trimColumns;
    }

    public void setTrimColumns(List<String> trimColumns) {
        this.trimColumns = trimColumns;
    }
}
