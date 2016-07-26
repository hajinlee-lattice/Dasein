package com.latticeengines.metadata.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.domain.exposed.util.DataRuleUtils;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.metadata.service.ModelReviewService;
import com.latticeengines.metadata.service.RuleResultService;

@Component("modelReviewService")
public class ModelReviewServiceImpl implements ModelReviewService {

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private RuleResultService ruleResultService;

    @Override
    public ModelReviewData getReviewData(String customerSpace, String modelId, String eventTableName) {
        Table eventTable = metadataService.getTable(CustomerSpace.parse(customerSpace), eventTableName);
        List<DataRule> rules = eventTable.getDataRules();
        DataRuleUtils.populateDataRuleDisplayNameAndDescriptions(rules);

        List<ColumnRuleResult> columnResults = ruleResultService.findColumnResults(modelId);
        List<RowRuleResult> rowResults = ruleResultService.findRowResults(modelId);

        Map<String, ColumnRuleResult> ruleNameToColumnRuleResults = new HashMap<>();
        for (ColumnRuleResult columnRuleResult : columnResults) {
            ruleNameToColumnRuleResults.put(columnRuleResult.getDataRuleName(), columnRuleResult);
        }

        Map<String, RowRuleResult> ruleNameToRowRuleResults = new HashMap<>();
        for (RowRuleResult rowRuleResult : rowResults) {
            ruleNameToRowRuleResults.put(rowRuleResult.getDataRuleName(), rowRuleResult);
        }

        filterColumnsToRemediate(rules, ruleNameToColumnRuleResults);

        ModelReviewData reviewData = new ModelReviewData();
        reviewData.setDataRules(rules);
        reviewData.setRuleNameToColumnRuleResults(ruleNameToColumnRuleResults);
        reviewData.setRuleNameToRowRuleResults(ruleNameToRowRuleResults);

        return reviewData;
    }

    private void filterColumnsToRemediate(List<DataRule> rules, Map<String, ColumnRuleResult> ruleNameToColumnRuleResults) {
        for (DataRule rule : rules) {
            ColumnRuleResult columnResult = ruleNameToColumnRuleResults.get(rule.getName());
            if (columnResult != null && columnResult.getFlaggedColumnNames() != null) {
                Set<String> columnsToRemediateSet = new HashSet<>(rule.getColumnsToRemediate());
                Set<String> flaggedColumnNames = new HashSet<>(columnResult.getFlaggedColumnNames());
                columnsToRemediateSet.retainAll(flaggedColumnNames);
                List<String> columnsToRemediate = new ArrayList<>(columnsToRemediateSet);
                rule.setColumnsToRemediate(columnsToRemediate);
            }
        }

    }
}
