package com.latticeengines.metadata.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
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
        populateDataRuleDisplayNameAndDescriptions(rules);

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

        ModelReviewData reviewData = new ModelReviewData();
        reviewData.setDataRules(rules);
        reviewData.setRuleNameToColumnRuleResults(ruleNameToColumnRuleResults);
        reviewData.setRuleNameToRowRuleResults(ruleNameToRowRuleResults);

        return reviewData;
    }

    private void populateDataRuleDisplayNameAndDescriptions(List<DataRule> dataRules) {
        for (DataRule rule : dataRules) {
            switch (rule.getName()) {
            case "UniqueValueCountDS":
                rule.setDescription("Unique value count in column - Integrated from Profiling");
                rule.setDisplayName("Unique Value Count");
                break;
            case "PopulatedRowCountDS":
                rule.setDescription("Populated Row Count - Integrated from Profiling (certain value exceeds x%)");
                rule.setDisplayName("Populated Row Count");
                break;
            case "OverlyPredictiveDS":
                rule.setDescription("Overly predictive single category / value range");
                rule.setDisplayName("Overly Predictive Columns");
                break;
            case "LowCoverageDS":
                rule.setDescription("Low coverage (empty exceeds x%)");
                rule.setDisplayName("Low Coverage");
                break;
            case "NullIssueDS":
                rule.setDescription("Positively predictive nulls");
                rule.setDisplayName("Positively Predictive Nulls");
                break;
            case "HighlyPredictiveSmallPopulationDS":
                rule.setDescription("High predictive, low population");
                rule.setDisplayName("High Predictive Low Population");
                break;

            default:
                break;
            }
        }
    }

}
