package com.latticeengines.domain.exposed.modelreview;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;


public class ModelReviewData {

    @JsonProperty
    private List<DataRule> dataRules;

    @JsonProperty
    private Map<String, ColumnRuleResult> ruleNameToColumnRuleResults;

    @JsonProperty
    private Map<String, RowRuleResult> ruleNameToRowRuleResults;

    public List<DataRule> getDataRules() {
        return dataRules;
    }

    public void setDataRules(List<DataRule> dataRules) {
        this.dataRules = dataRules;
    }

    public Map<String, ColumnRuleResult> getRuleNameToColumnRuleResults() {
        return ruleNameToColumnRuleResults;
    }

    public void setRuleNameToColumnRuleResults(Map<String, ColumnRuleResult> ruleNameToColumnRuleResults) {
        this.ruleNameToColumnRuleResults = ruleNameToColumnRuleResults;
    }

    public Map<String, RowRuleResult> getRuleNameToRowRuleResults() {
        return ruleNameToRowRuleResults;
    }

    public void setRuleNameToRowRuleResults(Map<String, RowRuleResult> ruleNameToRowRuleResults) {
        this.ruleNameToRowRuleResults = ruleNameToRowRuleResults;
    }

}
