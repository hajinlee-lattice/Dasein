package com.latticeengines.metadata.service.impl;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.RuleResultService;

public class RuleResultServiceImplTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private RuleResultService ruleResultService;

    private static int counter = 1;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
    }

    @Test(groups = "functional")
    public void testColumnResults() {
        String modelId = "test123";
        Tenant tenant1 = tenantEntityMgr.findByTenantId(customerSpace1);

        List<ColumnRuleResult> results = new ArrayList<>();
        results.add(generateColumnResult(modelId, tenant1));
        results.add(generateColumnResult(modelId, tenant1));
        ruleResultService.createColumnResults(results);

        List<ColumnRuleResult> retrievedResults = ruleResultService.findColumnResults(modelId);

        assertEquals(retrievedResults.size(), results.size());
        assertEquals(retrievedResults, results);
        JsonUtils.serialize(retrievedResults); // Confirm this works without
                                               // exception
        ruleResultService.deleteColumnResults(results);
    }

    private ColumnRuleResult generateColumnResult(String modelId, Tenant tenant) {
        ColumnRuleResult result = new ColumnRuleResult();
        result.setDataRuleName("ColumnRule" + counter++);
        List<String> flaggedColumnNames = new ArrayList<>();
        flaggedColumnNames.add("Column" + counter++);
        flaggedColumnNames.add("Column" + counter++);
        flaggedColumnNames.add("Column" + counter++);
        result.setFlaggedColumnNames(flaggedColumnNames);
        result.setModelId(modelId);
        result.setTenant(tenant);
        return result;
    }

    @Test(groups = "functional")
    public void testRowResults() {
        String modelId = "test123";
        Tenant tenant1 = tenantEntityMgr.findByTenantId(customerSpace1);

        List<RowRuleResult> results = new ArrayList<>();
        results.add(generateRowResult(modelId, tenant1));
        results.add(generateRowResult(modelId, tenant1));
        ruleResultService.createRowResults(results);

        List<RowRuleResult> retrievedResults = ruleResultService.findRowResults(modelId);

        assertEquals(retrievedResults.size(), results.size());
        assertEquals(retrievedResults, results);
        JsonUtils.serialize(retrievedResults); // Confirm this works without
                                               // exception
        ruleResultService.deleteRowResults(results);
    }

    private RowRuleResult generateRowResult(String modelId, Tenant tenant) {
        RowRuleResult result = new RowRuleResult();
        result.setDataRuleName("RowRule" + counter++);
        String rowId = "row" + counter++;
        String rowId2 = "row" + counter++;
        List<String> flaggedColumnNames = new ArrayList<>();
        flaggedColumnNames.add("Column" + counter++);
        flaggedColumnNames.add("Column" + counter++);
        flaggedColumnNames.add("Column" + counter++);
        Map<String, List<String>> flaggedRowIdAndColumnNames = new HashMap<>();
        flaggedRowIdAndColumnNames.put(rowId, flaggedColumnNames);
        flaggedRowIdAndColumnNames.put(rowId2, flaggedColumnNames);
        Map<String, Boolean> flaggedRowIdAndPositiveEvent = new HashMap<>();
        flaggedRowIdAndPositiveEvent.put(rowId, true);
        flaggedRowIdAndPositiveEvent.put(rowId2, false);
        result.setFlaggedRowIdAndColumnNames(flaggedRowIdAndColumnNames);
        result.setFlaggedRowIdAndPositiveEvent(flaggedRowIdAndPositiveEvent);

        result.setModelId(modelId);
        result.setTenant(tenant);
        return result;
    }

}
