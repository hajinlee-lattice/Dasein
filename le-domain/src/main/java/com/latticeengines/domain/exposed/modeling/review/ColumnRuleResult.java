package com.latticeengines.domain.exposed.modeling.review;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ColumnRuleResult extends BaseRuleResult {

    private Set<String> columnNames;

    @JsonProperty
    public Set<String> getColumnNames() {
        return columnNames;
    }

    @JsonProperty
    public void setColumnNames(Set<String> columnNames) {
        this.columnNames = columnNames;
    }
}
