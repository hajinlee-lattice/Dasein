package com.latticeengines.metadata.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Triple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

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
    public ModelReviewData getReviewData(String modelId, String eventTableName) {
        Table eventTable = metadataService.getTable(null, eventTableName);
        List<DataRule> rules = eventTable.getDataRules();

        List<ColumnRuleResult> columnResults = ruleResultService.findColumnResults(modelId);
        List<RowRuleResult> rowResults = ruleResultService.findRowResults(modelId);

        Map<String, ColumnRuleResult> ruleNameToColumnRuleResults = new HashMap<>();
        for (ColumnRuleResult columnRuleResult : columnResults) {
            columnRuleResult.setFlaggedItemCount(columnRuleResult.getFlaggedColumnNames().size());
            ruleNameToColumnRuleResults.put(columnRuleResult.getDataRuleName(), columnRuleResult);
        }

        Map<String, RowRuleResult> ruleNameToRowRuleResults = new HashMap<>();
        for (RowRuleResult rowRuleResult : rowResults) {
            rowRuleResult.setFlaggedItemCount(rowRuleResult.getFlaggedRowIdAndColumnNames().size());

            int positiveEventCount = 0;
            for (Boolean val : rowRuleResult.getFlaggedRowIdAndPositiveEvent().values()) {
                if (val) {
                    positiveEventCount++;
                }
            }
            rowRuleResult.setNumPositiveEvents(positiveEventCount);

            ruleNameToRowRuleResults.put(rowRuleResult.getDataRuleName(), rowRuleResult);
        }

        ModelReviewData reviewData = new ModelReviewData();
        reviewData.setDataRules(rules);
        reviewData.setRuleNameToColumnRuleResults(ruleNameToColumnRuleResults);
        reviewData.setRuleNameToRowRuleResults(ruleNameToRowRuleResults);

        return reviewData;
    }

    @SuppressWarnings("unused")
    private ModelReviewData generateStubData() {
        Triple<List<DataRule>, Map<String, ColumnRuleResult>, Map<String, RowRuleResult>> masterList = getMasterList();

        ModelReviewData reviewData = new ModelReviewData();
        reviewData.setDataRules(masterList.getLeft());
        reviewData.setRuleNameToColumnRuleResults(masterList.getMiddle());
        reviewData.setRuleNameToRowRuleResults(masterList.getRight());

        return reviewData;
    }

    @SuppressWarnings("unchecked")
    private Triple<List<DataRule>, Map<String, ColumnRuleResult>, Map<String, RowRuleResult>> getMasterList() {
        List<Triple<String, String, Boolean>> masterColumnConfig = new ArrayList<>();
        List<Triple<String, String, Boolean>> masterRowConfig = new ArrayList<>();

        Triple<String, String, Boolean> overlyPredictiveColumns = Triple.of("Overly Predictive Columns",
                "overly predictive single category / value range", false);
        masterColumnConfig.add(overlyPredictiveColumns);
        Triple<String, String, Boolean> lowCoverage = Triple
                .of("Low Coverage", "Low coverage (empty exceeds x%)", false);
        masterColumnConfig.add(lowCoverage);
        Triple<String, String, Boolean> populatedRowCount = Triple.of("Populated Row Count",
                "Populated Row Count - Integrated from Profiling (certain value exceeds x%) ", false);
        masterColumnConfig.add(populatedRowCount);
        Triple<String, String, Boolean> positivelyPredictiveNulls = Triple.of("Positively Predictive Nulls",
                "Positively predictive nulls", false);
        masterColumnConfig.add(positivelyPredictiveNulls);
        Triple<String, String, Boolean> uniqueValueCount = Triple.of("Unique Value Count",
                "Unique value count in column - Integrated from Profiling", false);
        masterColumnConfig.add(uniqueValueCount);
        Triple<String, String, Boolean> publicDomains = Triple.of("Public Domains",
                "Exclude Records with Public Domains ", false);
        masterRowConfig.add(publicDomains);
        Triple<String, String, Boolean> customDomains = Triple.of("Custom Domains", "Exclude specific domain(s)", false);
        masterRowConfig.add(customDomains);
        Triple<String, String, Boolean> oneRecordPerDomain = Triple.of("One Record Per Domain", "One Record Per Domain",
                false);
        masterRowConfig.add(oneRecordPerDomain);
        Triple<String, String, Boolean> oneLeadPerAccount = Triple.of("One Lead Per Account", "One Lead Per Account",
                false);
        masterRowConfig.add(oneLeadPerAccount);
        Triple<String, String, Boolean> highPredictiveLowPopulation = Triple.of("High Predictive Low Population",
                "High predictive, low population", false);
        masterRowConfig.add(highPredictiveLowPopulation);

        List<DataRule> masterRuleList = new ArrayList<>();
        Map<String, ColumnRuleResult> columnResults = new HashMap<>();
        for (Triple<String, String, Boolean> config : masterColumnConfig) {
            DataRule rule = generateDataRule(config);
            masterRuleList.add(rule);
            ColumnRuleResult columnResult = new ColumnRuleResult();
            columnResult.setDataRuleName(rule.getName());
            if (rule.getName().equals("UniqueValueCount")) {
                List<String> flaggedColumns = new ArrayList<>();
                flaggedColumns.add("SomeColumnA");
                flaggedColumns.add("AnotherColumnB");
                columnResult.setFlaggedColumnNames(flaggedColumns);
            } else {
                columnResult.setFlaggedColumnNames(Collections.EMPTY_LIST);
            }
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
            rowResult.setFlaggedRowIdAndColumnNames(Collections.EMPTY_MAP);
            rowResult.setNumPositiveEvents(0);
            rowResults.put(rule.getName(), rowResult);
        }

        return Triple.of(masterRuleList, columnResults, rowResults);
    }

    @SuppressWarnings("unchecked")
    private DataRule generateDataRule(Triple<String, String, Boolean> config) {
        DataRule rule = new DataRule();
        rule.setName(StringUtils.trimAllWhitespace(config.getLeft()));
        rule.setDisplayName(config.getLeft());
        rule.setDescription(config.getMiddle());
        rule.setFrozenEnablement(config.getRight());
        rule.setColumnsToRemediate(Collections.EMPTY_LIST);
        if (rule.getName().equals("CustomDomains")) {
            Map<String, String> props = new HashMap<>();
            props.put("domains", "company.com, anothersite.com, abc.com");
            rule.setProperties(props);
        } else {
            rule.setProperties(Collections.EMPTY_MAP);
        }
        return rule;
    }

}
