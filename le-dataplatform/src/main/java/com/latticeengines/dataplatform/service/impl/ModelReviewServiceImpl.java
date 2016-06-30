package com.latticeengines.dataplatform.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Triple;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.ModelReviewService;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;

import edu.emory.mathcs.backport.java.util.Collections;

@Component("modelReviewService")
public class ModelReviewServiceImpl implements ModelReviewService {

    @Override
    public ModelReviewData getReviewData(String modelId) {
        Triple<List<DataRule>, Map<String, ColumnRuleResult>, Map<String, RowRuleResult>> masterList = getMasterList();

        // TODO default

        ModelReviewData reviewData = new ModelReviewData();
        reviewData.setDataRules(masterList.getLeft());
        reviewData.setRuleNameToColumnRuleResults(masterList.getMiddle());
        reviewData.setRuleNameToRowRuleResults(masterList.getRight());

        return reviewData;
    }

    // TODO bernard should be combination of rulepipeline.json and the current
    // model's results
    @SuppressWarnings("unchecked")
    private Triple<List<DataRule>, Map<String, ColumnRuleResult>, Map<String, RowRuleResult>> getMasterList() {
        List<Triple<String, String, Boolean>> masterColumnConfig = new ArrayList<>();
        List<Triple<String, String, Boolean>> masterRowConfig = new ArrayList<>();

        Triple<String, String, Boolean> overlyPredictiveColumns = Triple.of("OverlyPredictiveColumns",
                "overly predictive single category / value range", false);
        masterColumnConfig.add(overlyPredictiveColumns);
        Triple<String, String, Boolean> lowCoverage = Triple
                .of("LowCoverage", "Low coverage (empty exceeds x%)", false);
        masterColumnConfig.add(lowCoverage);
        Triple<String, String, Boolean> populatedRowCount = Triple.of("PopulatedRowCount",
                "Populated Row Count - Integrated from Profiling (certain value exceeds x%) ", false);
        masterColumnConfig.add(populatedRowCount);
        Triple<String, String, Boolean> positivelyPredictiveNulls = Triple.of("PositivelyPredictiveNulls",
                "Positively predictive nulls", false);
        masterColumnConfig.add(positivelyPredictiveNulls);
        Triple<String, String, Boolean> uniqueValueCount = Triple.of("UniqueValueCount",
                "Unique value count in column - Integrated from Profiling", false);
        masterColumnConfig.add(uniqueValueCount);
        Triple<String, String, Boolean> publicDomains = Triple.of("PublicDomains",
                "Exclude Records with Public Domains ", false);
        masterRowConfig.add(publicDomains);
        Triple<String, String, Boolean> customDomains = Triple.of("CustomDomains", "Exclude specific domain(s)", false);
        masterRowConfig.add(customDomains);
        Triple<String, String, Boolean> oneRecordPerDomain = Triple.of("OneRecordPerDomain", "One Record Per Domain",
                false);
        masterRowConfig.add(oneRecordPerDomain);
        Triple<String, String, Boolean> oneLeadPerAccount = Triple.of("OneLeadPerAccount", "One Lead Per Account",
                false);
        masterRowConfig.add(oneLeadPerAccount);
        Triple<String, String, Boolean> highPredictiveLowPopulation = Triple.of("HighPredictiveLowPopulation",
                "High predictive, low population", false);
        masterRowConfig.add(highPredictiveLowPopulation);

        List<DataRule> masterRuleList = new ArrayList<>();
        Map<String, ColumnRuleResult> columnResults = new HashMap<>();
        for (Triple<String, String, Boolean> config : masterColumnConfig) {
            DataRule rule = generateDataRule(config);
            masterRuleList.add(rule);
            ColumnRuleResult columnResult = new ColumnRuleResult();
            columnResult.setDataRuleName(rule.getName());
            columnResult.setFlaggedColumnNames(Collections.emptyList());
            columnResult.setFlaggedColumns(Collections.emptyList());
            columnResult.setFlaggedItemCount(columnResult.getFlaggedColumnNames().size());
            columnResults.put(rule.getName(), columnResult);
        }

        Map<String, RowRuleResult> rowResults = new HashMap<>();
        for (Triple<String, String, Boolean> config : masterRowConfig) {
            DataRule rule = generateDataRule(config);
            masterRuleList.add(rule);
            RowRuleResult rowResult = new RowRuleResult();
            rowResult.setDataRuleName(rule.getName());
            rowResult.setFlaggedItemCount(0);
            rowResult.setFlaggedRowIdAndColumnNames(Collections.emptyMap());
            rowResult.setNumPositiveEvents(0);
            rowResults.put(rule.getName(), rowResult);
        }

        return Triple.of(masterRuleList, columnResults, rowResults);
    }

    @SuppressWarnings("unchecked")
    private DataRule generateDataRule(Triple<String, String, Boolean> config) {
        DataRule rule = new DataRule();
        rule.setName(config.getLeft());
        rule.setDescription(config.getMiddle());
        rule.setFrozenEnablement(config.getRight());
        rule.setColumnsToRemediate(Collections.emptyList());
        rule.setProperties(Collections.emptyMap());
        return rule;
    }

}
