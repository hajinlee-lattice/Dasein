package com.latticeengines.domain.exposed.modeling.review;

import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RowRuleResult extends BaseRuleResult {

    private Map<String, Set<String>> rowIdBecauseOfColumns;

    @JsonProperty
    public Map<String, Set<String>> getRowIdBecauseOfColumns() {
        return rowIdBecauseOfColumns;
    }

    @JsonProperty
    public void setRowIdBecauseOfColumns(Map<String, Set<String>> rowIdBecauseOfColumns) {
        this.rowIdBecauseOfColumns = rowIdBecauseOfColumns;
    }
}
