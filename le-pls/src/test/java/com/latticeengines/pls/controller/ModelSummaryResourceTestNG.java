package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelAlerts;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.impl.ModelSummaryParser;

/**
 * This test has two users with particular privileges:
 * 
 * rgonzalez - View_PLS_Reporting for tenant1 bnguyen - View_PLS_Reporting,
 * View_PLS_Models for tenant2
 * 
 * It ensures that rgonzalez cannot access any model summaries since it does not
 * have the View_PLS_Models right.
 * 
 * It also ensures that bnguyen can indeed access the model summaries since it
 * does have the View_PLS_Models right.
 * 
 * It also ensures that updates can only be done by bnguyen since this user has
 * Edit_PLS_Models right.
 * 
 * @author rgonzalez
 * 
 */
public class ModelSummaryResourceTestNG extends PlsFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ModelSummaryResourceTestNG.class);
    private String tenantId;

    private String modelId;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Value("${pls.default.buyerinsights.num.predictors}")
    private int defaultBiPredictorNum;

    @Autowired
    private Configuration yarnConfiguration;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setupMarketoEloquaTestEnvironment();

        tenantId = eloquaTenant.getId();
        modelId = eloquaModelId;

        String dir = modelingServiceHdfsBaseDir + "/" + tenantId + "/models/ANY_TABLE/" + eloquaModelId + "/container_01/";
        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/modelsummary-marketo.json");
        URL metadataDiagnosticsUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/metadata-diagnostics.json");
        URL dataDiagnosticsUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/diagnostics.json");
        URL rfMoelUrl = ClassLoader.getSystemResource("com/latticeengines/pls/functionalframework/rf_model.txt");
        URL topPredictorUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/topPredictor_model.csv");

        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir + "/enhancements");
        HdfsUtils
                .copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/enhancements/modelsummary.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, metadataDiagnosticsUrl.getFile(), dir
                + "/metadata-diagnostics.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataDiagnosticsUrl.getFile(), dir + "/diagnostics.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, rfMoelUrl.getFile(), dir + "/rf_model.txt");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, topPredictorUrl.getFile(), dir + "/topPredictor_model.csv");
    }

    @BeforeMethod(groups = { "functional" })
    public void beforeMethod() {
        // using admin session by default
        switchToSuperAdmin();
    }

    @AfterClass(groups = { "functional" })
    public void teardown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + "/" + tenantId);
    }

    @Test(groups = { "functional" })
    public void assertNonexistedModelAlertGet403() {
        try {
            restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/alerts/someModel", ModelAlerts.class);
            Assert.fail("Should have thrown an exception.");
        } catch (Exception e) {
            HttpStatus status = getErrorHandler().getStatusCode();
            assertEquals(status, HttpStatus.FORBIDDEN,
                    "Should return forbidden 403, but got " + getErrorHandler().getResponseString());
        }
    }

    @Test(groups = { "functional" })
    public void testGenerateModelAlert() {
        String response = null;
        try {
            response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/alerts/" + eloquaModelId,
                    String.class);
        } catch (Exception e) {
            System.out.println(ExceptionUtils.getFullStackTrace(e));
            Assert.fail("Should NOT have thrown an exception.");
        }
        assertNotNull(response);
    }

    @Test(groups = { "functional" })
    public void deleteModelSummaryNoEditPlsModelsRight() {
        switchToExternalAdmin();
        assertDeleteModelSummaryGet403();
    }

    @Test(groups = { "functional" })
    public void mockDeleteModelSummaryHasEditPlsModelsRight() {
        switchToInternalUser();
        assertDeleteModelSummaryGet500();

        switchToExternalUser();
        assertDeleteModelSummaryGet500();
    }
    
    private void assertDeleteModelSummaryGet500() {
        boolean exception = false;
        try {
            restTemplate.delete(getRestAPIHostPort() + "/pls/modelsummaries/123");
        } catch (Exception e) {
            exception = true;
            HttpStatus status = getErrorHandler().getStatusCode();
            assertEquals(status, HttpStatus.INTERNAL_SERVER_ERROR,
                    "Should return 500, but got " + getErrorHandler().getResponseString());
            assertTrue(e.getMessage().contains("Model with id 123 not found"));
        }
        assertTrue(exception);
    }
    
    private void assertDeleteModelSummaryGet403() {
        boolean exception = false;
        try {
            restTemplate.delete(getRestAPIHostPort() + "/pls/modelsummaries/123");
        } catch (Exception e) {
            exception = true;
            HttpStatus status = getErrorHandler().getStatusCode();
            assertEquals(status, HttpStatus.FORBIDDEN,
                    "Should return forbidden 403, but got " + getErrorHandler().getResponseString());
        }
        assertTrue(exception);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = { "functional" })
    public void getModelSummariesAndPredictorsHasViewPlsModelsRight() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"),
                ModelSummary.class);
        assertNotNull(summary.getDetails());

        List<Predictor> predictors = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/modelsummaries/predictors/all/" + summary.getId(), List.class);
        assertNotNull(predictors);
        assertEquals(predictors.size(), 183);

        List<Predictor> predictorsForBi = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/modelsummaries/predictors/bi/" + summary.getId(), List.class);
        assertNotNull(predictorsForBi);
        assertEquals(predictorsForBi.size(), defaultBiPredictorNum);
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "getModelSummariesAndPredictorsHasViewPlsModelsRight" })
    public void testUpdateModelSummary() {
        switchToSuperAdmin();
        assertChangeModelDisplayNameSuccess();

        switchToInternalAdmin();
        assertChangeModelDisplayNameSuccess();

        switchToInternalAdmin();
        assertChangeModelDisplayNameFail();

        switchToInternalUser();
        assertChangeModelDisplayNameGet403();

        switchToExternalAdmin();
        assertChangeModelDisplayNameGet403();

        switchToExternalUser();
        assertChangeModelDisplayNameGet403();
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "testUpdateModelSummary" })
    public void testUpdatePredictors() {
        switchToSuperAdmin();
        assertUpdatePredictorsSuccess();

        switchToInternalAdmin();
        assertUpdatePredictorsSuccess();

        switchToInternalUser();
        assertUpdatePredictorsGet403();

        switchToExternalAdmin();
        assertUpdatePredictorsGet403();

        switchToExternalUser();
        assertUpdatePredictorsGet403();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void assertUpdatePredictorsSuccess() {

        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/predictors/all/"
                + modelId, List.class);
        assertNotNull(response);
        int size = response.size();
        assertEquals(size, 183);

        Map<String, Object> firstPredictor = (Map) response.get(0);
        Map<String, Object> lastPredictor = (Map) response.get(size - 1);
        Map<String, Object> secondLastPredictor = (Map) response.get(size - 2);
        boolean firstPredictorPredictorIsUsed = Boolean.parseBoolean(firstPredictor.get("UsedForBuyerInsights")
                .toString());
        boolean lastPredictorIsUsed = Boolean.parseBoolean(lastPredictor.get("UsedForBuyerInsights").toString());
        boolean secondLastPredictorIsUsed = Boolean.parseBoolean(lastPredictor.get("UsedForBuyerInsights").toString());

        AttributeMap attrMap = new AttributeMap();
        attrMap.put(firstPredictor.get("Name").toString(),
                PredictorStatus.getflippedStatusCode(firstPredictorPredictorIsUsed));
        attrMap.put(lastPredictor.get("Name").toString(), PredictorStatus.getflippedStatusCode(lastPredictorIsUsed));
        attrMap.put(secondLastPredictor.get("Name").toString(),
                PredictorStatus.getflippedStatusCode(secondLastPredictorIsUsed));

        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/predictors/" + modelId, attrMap, new HashMap<>());

        List<Predictor> predictorsForBi = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/modelsummaries/predictors/bi/" + modelId, List.class);
        assertNotNull(predictorsForBi);
        assertEquals(predictorsForBi.size(), defaultBiPredictorNum + 1);

        response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/predictors/all/" + modelId,
                List.class);
        firstPredictor = (Map) response.get(0);
        lastPredictor = (Map) response.get(size - 1);
        secondLastPredictor = (Map) response.get(size - 2);
        firstPredictorPredictorIsUsed = Boolean.parseBoolean(firstPredictor.get("UsedForBuyerInsights").toString());
        lastPredictorIsUsed = Boolean.parseBoolean(lastPredictor.get("UsedForBuyerInsights").toString());
        secondLastPredictorIsUsed = Boolean.parseBoolean(lastPredictor.get("UsedForBuyerInsights").toString());

        AttributeMap newAttrMap = new AttributeMap();
        newAttrMap.put(firstPredictor.get("Name").toString(),
                PredictorStatus.getflippedStatusCode(firstPredictorPredictorIsUsed));
        newAttrMap.put(lastPredictor.get("Name").toString(), PredictorStatus.getflippedStatusCode(lastPredictorIsUsed));
        newAttrMap.put(secondLastPredictor.get("Name").toString(),
                PredictorStatus.getflippedStatusCode(secondLastPredictorIsUsed));

        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/predictors/" + modelId, newAttrMap,
                new HashMap<>());

        predictorsForBi = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/predictors/bi/"
                + modelId, List.class);
        assertNotNull(predictorsForBi);
        assertEquals(predictorsForBi.size(), defaultBiPredictorNum);

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void assertUpdatePredictorsGet403() {
        boolean exception = false;
        try {
            List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/predictors/all/"
                    + modelId, List.class);
            assertNotNull(response);
            int size = response.size();
            assertEquals(size, 183);

            Map<String, Object> firstPredictor = (Map) response.get(0);
            boolean firstPredictorPredictorIsUsed = Boolean.parseBoolean(firstPredictor.get("UsedForBuyerInsights")
                    .toString());
            AttributeMap attrMap = new AttributeMap();
            attrMap.put(firstPredictor.get("Name").toString(),
                    PredictorStatus.getflippedStatusCode(firstPredictorPredictorIsUsed));
            restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/predictors/" + modelId, attrMap,
                    new HashMap<>());
        } catch (Exception e) {
            exception = true;
            HttpStatus status = getErrorHandler().getStatusCode();
            assertEquals(status, HttpStatus.FORBIDDEN,
                    "Should return forbidden 403, but got " + getErrorHandler().getResponseString());
        }
        assertTrue(exception);
    }

    @Deprecated
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = { "functional" }, dependsOnMethods = { "testUpdateModelSummary" })
    public void updateAsDeletedModelSummaryHasEditPlsModelsRight() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);

        Map<String, String> map = (Map) response.get(0);
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsInactive");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());

        attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsDeleted");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"),
                ModelSummary.class);
        assertNull(summary);

        response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        int length1 = response.size();
        response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/?selection=all", List.class);
        assertNotNull(response);
        int length2 = response.size();
        assertEquals(length1 + 1, length2);

        attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsInactive");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());

        attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsActive");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = { "functional" }, dependsOnMethods = { "updateAsDeletedModelSummaryHasEditPlsModelsRight" })
    public void deleteModelSummaryHasEditPlsModelsRight() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        restTemplate.delete(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"));
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/modelsummaries/ms-8e3a9d8c-3bc1-4d21-9c91-0af28afc5c9a", ModelSummary.class);
        assertNull(summary);
    }

    @Test(groups = { "functional" })
    public void testPostModelSummariesNoCreatePlsModelsRight() throws IOException {
        switchToSuperAdmin();
        assertCreateModelSummariesSuccess();

        switchToInternalAdmin();
        assertCreateModelSummaryGet403();

        switchToInternalUser();
        assertCreateModelSummaryGet403();

        switchToExternalAdmin();
        assertCreateModelSummaryGet403();

        switchToExternalUser();
        assertCreateModelSummaryGet403();
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional" })
    public void postModelSummariesUsingRaw() throws IOException {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        int originalNumModels = response.size();
        ModelSummary data = new ModelSummary();
        InputStream modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(
                "com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");
        String contents = new String(IOUtils.toByteArray(modelSummaryFileAsStream));
        String uuid = eloquaModelId.replace("-" + eloquaModelName, "").replace(modelIdPrefix, "");
        contents = contents.replace("{uuid}", uuid);
        contents = contents.replace("{tenantId}", eloquaTenant.getId());
        contents = contents.replace("{modelName}", eloquaModelName);

        Tenant fakeTenant = new Tenant();
        fakeTenant.setId("FAKE_TENANT");
        fakeTenant.setName("Fake Tenant");
        fakeTenant.setPid(-1L);
        data.setTenant(fakeTenant);
        data.setRawFile(contents);

        ModelSummary newSummary = restTemplate.postForObject(getRestAPIHostPort() + "/pls/modelsummaries?raw=true",
                data, ModelSummary.class);
        assertNotNull(newSummary);
        response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), originalNumModels + 1);

        restTemplate.delete(getRestAPIHostPort() + "/pls/modelsummaries/" + newSummary.getId());
        response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertEquals(response.size(), originalNumModels);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void assertChangeModelDisplayNameSuccess() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        String originalDisplayName = map.get("DisplayName");
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("DisplayName", "xyz");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());

        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"),
                ModelSummary.class);
        assertEquals(summary.getDisplayName(), "xyz");
        assertNotNull(summary.getDetails());

        attrMap.put("DisplayName", originalDisplayName);
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());
        summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"),
                ModelSummary.class);
        assertEquals(summary.getDisplayName(), originalDisplayName);
    }

    @SuppressWarnings({ "rawtypes" })
    private void assertChangeModelDisplayNameFail() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("DisplayName", "xyz");
        HttpEntity<AttributeMap> requestEntity = new HttpEntity<>(attrMap);
        ResponseEntity<Boolean> responseEntity = restTemplate.exchange(getRestAPIHostPort() + "/pls/modelsummaries/"
                + "ms__4192dfb1-d78c-4521-80d5-cebf477b2978-Ron’s_Mo", HttpMethod.PUT, requestEntity, Boolean.class);
        Boolean responseDoc = responseEntity.getBody();
        Assert.assertFalse(responseDoc.booleanValue());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void assertChangeModelDisplayNameGet403() {
        boolean exception = false;
        try {
            List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
            assertNotNull(response);
            assertEquals(response.size(), 1);
            Map<String, String> map = (Map) response.get(0);
            AttributeMap attrMap = new AttributeMap();
            attrMap.put("DisplayName", "xyz");
            restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());
        } catch (Exception e) {
            exception = true;
            HttpStatus status = getErrorHandler().getStatusCode();
            assertEquals(status, HttpStatus.FORBIDDEN,
                    "Should return forbidden 403, but got " + getErrorHandler().getResponseString());
        }
        assertTrue(exception);
    }

    private void assertCreateModelSummaryGet403() {
        boolean exception = false;
        try {
            InputStream ins = getClass().getClassLoader().getResourceAsStream(
                    "com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");
            assertNotNull(ins, "Testing json file is missing");
            ModelSummary modelSummary = modelSummaryParser.parse("", new String(IOUtils.toByteArray(ins)));
            restTemplate.postForObject(getRestAPIHostPort() + "/pls/modelsummaries/", modelSummary, Boolean.class);
        } catch (Exception e) {
            exception = true;
            HttpStatus status = getErrorHandler().getStatusCode();
            assertEquals(status, HttpStatus.FORBIDDEN,
                    "Should return forbidden 403, but got " + getErrorHandler().getResponseString());
        }
        assertTrue(exception);
    }

    @SuppressWarnings("rawtypes")
    private void assertCreateModelSummariesSuccess() throws IOException {
        ModelSummary modelSummary = getDetails(eloquaTenant, "eloqua");
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        int originalNumModels = response.size();

        int version = 0;
        String possibleID = modelSummary.getId();
        String name = modelSummaryParser.parseOriginalName(modelSummary.getName());
        ModelSummary existingSummary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/"
                + possibleID, ModelSummary.class);
        while (existingSummary != null) {
            possibleID = modelSummary.getId().replace(name, name + "-" + String.format("%03d", ++version));
            existingSummary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + possibleID,
                    ModelSummary.class);
        }
        modelSummary.setId(possibleID);
        if (version > 0) {
            modelSummary.setName(modelSummary.getName().replace(name, name + "-" + String.format("%03d", version)));
        }

        ModelSummary newSummary = restTemplate.postForObject(getRestAPIHostPort() + "/pls/modelsummaries/",
                modelSummary, ModelSummary.class);
        assertNotNull(newSummary);
        response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), originalNumModels + 1);

        restTemplate.delete(getRestAPIHostPort() + "/pls/modelsummaries/" + newSummary.getId());
        response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertEquals(response.size(), originalNumModels);
    }

}
