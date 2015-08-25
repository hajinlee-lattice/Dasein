package com.latticeengines.upgrade.yarn;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.util.DateTime;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class YarnManagerTestNG extends UpgradeFunctionalTestNGBase {

    @Autowired
    private YarnManager yarnManager;

    @Autowired
    private Configuration yarnConfiguration;

    private String modelFileName;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        tearDown();
        String modelHdfsPath = YarnPathUtils.constructSingularIdModelsRoot(customerBase, CUSTOMER) + "/" + EVENT_TABLE
                + "/" + UUID + "/" + CONTAINER_ID + "/";
        HdfsUtils.rmdir(yarnConfiguration, modelHdfsPath);
        modelFileName = "PLSModel_2015-03-05_18-30";
        HdfsUtils.writeToFile(yarnConfiguration, modelHdfsPath + modelFileName + ".json", constructModelContent());
        HdfsUtils.writeToFile(yarnConfiguration, modelHdfsPath + modelFileName + ".csv", constructModelContent());
    }

    private String constructModelContent(){
        ObjectMapper objectMapper = new ObjectMapper();
        ModelingMetadata.DateTime dateTime = new ModelingMetadata.DateTime();
        dateTime.setDateTime("/Date(1435181458818)/");
        ObjectNode constructionTime = objectMapper.createObjectNode();
        constructionTime.putPOJO("ConstructionTime", dateTime.toString());
        ObjectNode constructionInfo = objectMapper.createObjectNode();
        constructionInfo.putPOJO("ConstructionInfo", constructionTime);
        ObjectNode summary = objectMapper.createObjectNode();
        summary.putPOJO("Summary", constructionInfo);
        return summary.toString();
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        deleteTupleIdCustomerRoot(CUSTOMER);
    }

    @Test(groups = "functional")
    public void testUpsertModels() throws Exception {
        yarnManager.moveModelsFromSingularToTupleId(CUSTOMER);
        String modelPath = YarnPathUtils.constructTupleIdModelsRoot(customerBase, CUSTOMER) + "/" + EVENT_TABLE + "/"
                + UUID + "/" + CONTAINER_ID;
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, modelPath),
                String.format("model %s for customer %s cannot be found at %s.", MODEL_GUID, CUSTOMER, modelPath));
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpsertModels" })
    public void testFixModelName() throws Exception {
        yarnManager.fixModelNameInTupleId(CUSTOMER, UUID);

        String modelPath = YarnPathUtils.constructTupleIdModelsRoot(customerBase, CUSTOMER) + "/" + EVENT_TABLE + "/"
                + UUID + "/" + CONTAINER_ID;
        String newModelJson = modelPath + "/" + modelFileName + "_model.json";
        String newModelCsv = modelPath + "/" + modelFileName + "_model.csv";

        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, newModelJson),
                String.format("renamed model.json for model %s of customer %s cannot be fixed at %s",
                        MODEL_GUID, CUSTOMER, modelPath));
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, newModelCsv),
                String.format("renamed model.csv for model %s of customer %s cannot be fixed at %s",
                        MODEL_GUID, CUSTOMER, modelPath));
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpsertModels" })
    public void testCheckModelSummary() {
        Assert.assertFalse(yarnManager.modelSummaryExistsInSingularId(CUSTOMER, UUID), "modelsummary should not exists");
    }

    @Test(groups = "functional", dependsOnMethods = { "testCheckModelSummary" })
    public void testGenerateModelSummary() {
        JsonNode summary = yarnManager.generateModelSummary(CUSTOMER, UUID);
        Assert.assertTrue(summary.has("ModelDetails"), "modelsummary.json should have ModelDetail");

        JsonNode detail = summary.get("ModelDetails");
        Assert.assertTrue(detail.has("Name"), "ModelDetail should have Name");
        Assert.assertTrue(detail.has("ConstructionTime"), "ModelDetail should have ConstructionTime");
        Assert.assertTrue(detail.has("LookupId"), "ModelDetail should have LookupId");
    }

    @Test(groups = "functional", dependsOnMethods = { "testGenerateModelSummary" })
    public void testUploadModelSummary() throws Exception {
        JsonNode summary = yarnManager.generateModelSummary(CUSTOMER, UUID);
        yarnManager.uploadModelsummary(CUSTOMER, UUID, summary);

        String summaryPath = YarnPathUtils.constructTupleIdModelsRoot(customerBase, CUSTOMER) + "/" + EVENT_TABLE + "/"
                + UUID + "/" + CONTAINER_ID + "/enhancements/modelsummary.json";
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, summaryPath), "Cannot find uploaded modelsummary.");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(HdfsUtils.getHdfsFileContents(yarnConfiguration, summaryPath));

        Assert.assertTrue(json.has("ModelDetails"), "modelsummary.json should have ModelDetail");

        JsonNode detail = json.get("ModelDetails");
        Assert.assertTrue(detail.has("Name"), "ModelDetail should have Name");
        Assert.assertTrue(detail.has("ConstructionTime"), "ModelDetail should have ConstructionTime");
        Assert.assertTrue(detail.has("LookupId"), "ModelDetail should have LookupId");

        try {
            new DateTime(detail.get("ConstructionTime").asLong());
        } catch (Exception e) {
            Assert.fail("Cannot parse ConstructionTime to a DateTime.", e);
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "testUploadModelSummary" })
    public void testDeleteModelSummary() throws Exception {
        yarnManager.deleteModelSummaryInTupleId(CUSTOMER, UUID);
        Assert.assertFalse(yarnManager.modelSummaryExistsInTupleId(CUSTOMER, UUID), "modelsummary should be deleted.");
    }

    private void deleteTupleIdCustomerRoot(String customer) {
        String customerPath = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, customer);
        try {
            HdfsUtils.rmdir(yarnConfiguration, customerPath);
        } catch (Exception e) {
            // ignore
        }
    }
}
