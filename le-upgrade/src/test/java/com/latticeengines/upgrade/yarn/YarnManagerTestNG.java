package com.latticeengines.upgrade.yarn;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class YarnManagerTestNG extends UpgradeFunctionalTestNGBase {

    @Autowired
    private YarnManager yarnManager;

    @Autowired
    private Configuration yarnConfiguration;

    @BeforeMethod(groups = "functional")
    public void beforeEach() throws Exception {
        yarnManager.deleteTupleIdCustomerRoot(CUSTOMER);
    }

    @AfterMethod(groups = "functional")
    public void afterEach() throws Exception {
        yarnManager.deleteTupleIdCustomerRoot(CUSTOMER);
    }

    @Test(groups = "functional")
    public void testDeleteTupleIdPath() throws Exception {
        String customerRoot = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, CUSTOMER);
        Assert.assertFalse(HdfsUtils.fileExists(yarnConfiguration, customerRoot));
    }

    @Test(groups = "functional")
    public void testCopyModel() throws Exception {
        yarnManager.copyModelsFromSingularToTupleId(CUSTOMER);
        yarnManager.fixModelName(CUSTOMER, MODEL_GUID);
        String modelPath = YarnPathUtils.constructTupleIdModelsRoot(customerBase, CUSTOMER)
                + "/" + EVENT_TABLE + "/" + UUID + "/" + CONTAINER_ID;
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, modelPath),
                String.format("model %s for customer %s cannot be found at %s.", MODEL_GUID, CUSTOMER, modelPath));
    }

    @Test(groups = "functional")
    public void testCopyData() throws Exception {
        yarnManager.copyModelsFromSingularToTupleId(CUSTOMER);

        String dataPath = YarnPathUtils.constructTupleIdModelsRoot(customerBase, CUSTOMER)
                + "/" + EVENT_TABLE;
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, dataPath),
                String.format("data for customer %s cannot be found at %s.", CUSTOMER, dataPath));
    }

    @Test(groups = "functional")
    public void testGenerateModelSummary() {
        JsonNode summary = yarnManager.generateModelSummary(CUSTOMER, MODEL_GUID);
        Assert.assertTrue(summary.has("ModelDetail"), "modelsummary.json should have ModelDetail");

        JsonNode detail = summary.get("ModelDetail");
        Assert.assertTrue(detail.has("Name"), "ModelDetail should have Name");
        Assert.assertTrue(detail.has("ConstructionTime"), "ModelDetail should have ConstructionTime");
        Assert.assertTrue(detail.has("LookupId"), "ModelDetail should have LookupId");
    }

    @Test(groups = "functional")
    public void testUploadModelSummary() throws Exception {
        JsonNode summary = yarnManager.generateModelSummary(CUSTOMER, MODEL_GUID);
        yarnManager.uploadModelsummary(CUSTOMER, MODEL_GUID, summary);

        String summaryPath = YarnPathUtils.constructTupleIdModelsRoot(customerBase, CUSTOMER)
                + "/" + EVENT_TABLE + "/" + UUID + "/" + CONTAINER_ID + "/enhancements/modelsummary.json";
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, summaryPath), "Cannot find uploaded modelsummary.");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(HdfsUtils.getHdfsFileContents(yarnConfiguration, summaryPath));

        Assert.assertTrue(json.has("ModelDetail"), "modelsummary.json should have ModelDetail");

        JsonNode detail = json.get("ModelDetail");
        Assert.assertTrue(detail.has("Name"), "ModelDetail should have Name");
        Assert.assertTrue(detail.has("ConstructionTime"), "ModelDetail should have ConstructionTime");
        Assert.assertTrue(detail.has("LookupId"), "ModelDetail should have LookupId");
    }

}


