package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.metadata.functionalframework.MetadataDeploymentTestNGBase;

public class RuleResultResourceDeploymentTestNG extends MetadataDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskResourceDeploymentTestNG.class);

    private static final String MODEL_ID = "ModelId_Test";

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() {
        super.setup();
    }

    @Test(groups = "deployment")
    public void testCreateColumnResults() throws IOException {
        List<ColumnRuleResult> columnRuleResults = new ArrayList<>();
        ColumnRuleResult result1 = new ColumnRuleResult();
        result1.setModelId(MODEL_ID);
        result1.setTenant(tenant1);
        result1.setDataRuleName("DataRuleName_Column_1");
        columnRuleResults.add(result1);

        ColumnRuleResult result2 = new ColumnRuleResult();
        result2.setModelId(MODEL_ID);
        result2.setTenant(tenant1);
        result2.setDataRuleName("DataRuleName_Column_2");
        columnRuleResults.add(result2);

        log.info("Create column results for "+ tenant1);
        boolean result = metadataProxy.createColumnResults(columnRuleResults);
        assertTrue(result);
    }

    @Test(groups = "deployment")
    public void testCreateRowResults() throws IOException {
        List<RowRuleResult> rowRuleResults = new ArrayList<>();

        log.info("Create row results for "+ tenant1);
        boolean result = metadataProxy.createRowResults(rowRuleResults);
        assertTrue(result);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateColumnResults", enabled = false)
    public void testGetColumnResults() throws IOException {
        List<ColumnRuleResult> columnRuleResults = metadataProxy.getColumnResults(MODEL_ID);
        assertNotNull(columnRuleResults);
        assertTrue(columnRuleResults.size() == 2);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateRowResults", enabled = false)
    public void testGetRowResults() throws IOException {
        List<RowRuleResult> rowRuleResults = metadataProxy.getRowResults(MODEL_ID);
        assertNotNull(rowRuleResults);
        assertTrue(rowRuleResults.size() == 0);
    }

    @Test(groups = "deployment")
    public void testGetReviewData() throws IOException {
        log.info("Get review data");
        ModelReviewData modelReviewData = metadataProxy.getReviewData(customerSpace1, MODEL_ID, TABLE1);

        assertNotNull(modelReviewData);
        assertEquals(modelReviewData.getDataRules().size(), 0);
    }
}
