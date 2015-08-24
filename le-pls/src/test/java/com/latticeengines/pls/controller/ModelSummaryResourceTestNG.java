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
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelAlerts;
import com.latticeengines.domain.exposed.pls.ModelSummary;
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

    private static final String MODEL_ID = "ms__8e3a9d8c-3bc1-4d21-9c91-0af28afc5c9a-PLSModel";

    private String tenantId;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setUpMarketoEloquaTestEnvironment();

        tenantId = testingTenants.get(0).getId();
        String dir = modelingServiceHdfsBaseDir + "/" + tenantId + "/models/ANY_TABLE/" + MODEL_ID + "/container_01/";
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
            String code = e.getMessage();
            assertEquals(code, "403");
        }
    }

    @Test(groups = { "functional" })
    public void testGenerateModelAlert() {
        String response = null;
        try {
            response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/alerts/" + MODEL_ID,
                    String.class);
        } catch (Exception e) {
            System.out.println(ExceptionUtils.getFullStackTrace(e));
            Assert.fail("Should NOT have thrown an exception.");
        }
        assertNotNull(response);
    }

    @Test(groups = { "functional" })
    public void deleteModelSummaryNoEditPlsModelsRight() {
        switchToInternalUser();
        assertDeleteModelSummaryGet403();

        switchToExternalAdmin();
        assertDeleteModelSummaryGet403();

        switchToExternalUser();
        assertDeleteModelSummaryGet403();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = { "functional" })
    public void getModelSummariesHasViewPlsModelsRight() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"),
                ModelSummary.class);
        assertNotNull(summary.getDetails());
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "getModelSummariesHasViewPlsModelsRight" })
    public void testUpdateModelSummary() {
        switchToSuperAdmin();
        assertChangeModelNameSuccess();

        switchToInternalAdmin();
        assertChangeModelNameSuccess();

        switchToInternalUser();
        assertChangeModelNameGet403();

        switchToExternalAdmin();
        assertChangeModelNameGet403();

        switchToExternalUser();
        assertChangeModelNameGet403();
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

        InputStream ins = getClass().getClassLoader().getResourceAsStream(
                "com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");
        assertNotNull(ins, "Testing json file is missing");

        ModelSummary data = new ModelSummary();
        Tenant fakeTenant = new Tenant();
        fakeTenant.setId("FAKE_TENANT");
        fakeTenant.setName("Fake Tenant");
        fakeTenant.setPid(-1L);
        data.setTenant(fakeTenant);
        data.setRawFile(new String(IOUtils.toByteArray(ins)));

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

    private void assertDeleteModelSummaryGet403() {
        boolean exception = false;
        try {
            restTemplate.delete(getRestAPIHostPort() + "/pls/modelsummaries/123");
        } catch (Exception e) {
            String code = e.getMessage();
            exception = true;
            assertEquals(code, "403");
        }
        assertTrue(exception);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void assertChangeModelNameSuccess() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        String originalName = map.get("Name");
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Name", "xyz");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());

        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"),
                ModelSummary.class);
        assertEquals(summary.getName(), "xyz");
        assertNotNull(summary.getDetails());

        attrMap.put("Name", originalName);
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());
        summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"),
                ModelSummary.class);
        assertEquals(summary.getName(), originalName);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void assertChangeModelNameGet403() {
        boolean exception = false;
        try {
            List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
            assertNotNull(response);
            assertEquals(response.size(), 1);
            Map<String, String> map = (Map) response.get(0);
            AttributeMap attrMap = new AttributeMap();
            attrMap.put("Name", "xyz");
            restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());
        } catch (Exception e) {
            String code = e.getMessage();
            exception = true;
            assertEquals(code, "403");
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
            String code = e.getMessage();
            exception = true;
            assertEquals(code, "403");
        }
        assertTrue(exception);
    }

    @SuppressWarnings("rawtypes")
    private void assertCreateModelSummariesSuccess() throws IOException {
        InputStream ins = getClass().getClassLoader().getResourceAsStream(
                "com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");
        assertNotNull(ins, "Testing json file is missing");
        ModelSummary modelSummary = modelSummaryParser.parse("", new String(IOUtils.toByteArray(ins)));
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
