package com.latticeengines.domain.exposed.modeling.review;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ModelReviewResults {

    private Map<DataRuleName, ColumnRuleResult> columnRuleResults;

    private Map<DataRuleName, RowRuleResult> rowRuleResults;

    @JsonProperty
    public Map<DataRuleName, ColumnRuleResult> getColumnRuleResults() {
        return columnRuleResults;
    }

    @JsonProperty
    public void setColumnRuleResults(Map<DataRuleName, ColumnRuleResult> columnRuleResults) {
        this.columnRuleResults = columnRuleResults;
    }

    @JsonProperty
    public Map<DataRuleName, RowRuleResult> getRowRuleResults() {
        return rowRuleResults;
    }

    @JsonProperty
    public void setRowRuleResults(Map<DataRuleName, RowRuleResult> rowRuleResults) {
        this.rowRuleResults = rowRuleResults;
    }
}
