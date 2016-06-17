package com.latticeengines.domain.exposed.modeling.review;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ModelReviewResults {

    private Map<DataRule, ColumnRuleResult> columnRuleResults;

    private Map<DataRule, RowRuleResult> rowRuleResults;

    @JsonProperty
    public Map<DataRule, ColumnRuleResult> getColumnRuleResults() {
        return columnRuleResults;
    }

    @JsonProperty
    public void setColumnRuleResults(Map<DataRule, ColumnRuleResult> columnRuleResults) {
        this.columnRuleResults = columnRuleResults;
    }

    @JsonProperty
    public Map<DataRule, RowRuleResult> getRowRuleResults() {
        return rowRuleResults;
    }

    @JsonProperty
    public void setRowRuleResults(Map<DataRule, RowRuleResult> rowRuleResults) {
        this.rowRuleResults = rowRuleResults;
    }
}
