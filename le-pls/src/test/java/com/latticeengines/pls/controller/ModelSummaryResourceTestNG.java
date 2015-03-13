package com.latticeengines.pls.controller;

import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;
import com.latticeengines.pls.globalauth.authentication.GlobalSessionManagementService;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * This test has two users with particular privileges:
 *
 * rgonzalez - View_PLS_Reporting for tenant1
 * bnguyen - View_PLS_Reporting, View_PLS_Models for tenant2
 *
 * It ensures that rgonzalez cannot access any model summaries since it does not
 * have the View_PLS_Models right.
 *
 * It also ensures that bnguyen can indeed access the model summaries since it does
 * have the View_PLS_Models right.
 *
 * It also ensures that updates can only be done by bnguyen since this user
 * has Edit_PLS_Models right.
 *
 * @author rgonzalez
 *
 */
public class ModelSummaryResourceTestNG extends PlsFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ModelSummaryResourceTestNG.class);

    private UserDocument adminDoc;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        setupUsers();

        Ticket ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));
        assertEquals(ticket.getTenants().size(), 2);
        assertNotNull(ticket);
        String tenant1 = ticket.getTenants().get(0).getId();
        String tenant2 = ticket.getTenants().get(1).getId();
        setupDb(tenant1, tenant2);

        adminDoc = loginAndAttachAdmin();
    }

    @AfterClass(groups = { "functional", "deployment" })
    public void tearDown() {
        globalAuthenticationService.discard(adminDoc.getTicket());
    }

    @BeforeMethod(groups = { "functional", "deployment" })
    public void beforeMethod() {
        // using admin session by default
        addAuthHeader.setAuthValue(adminDoc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
    }

    @Test(groups = { "functional", "deployment" })
    public void getModelSummariesNoViewPlsModelsRight() {
        UserDocument doc = loginAndAttach("rgonzalez");
        addAuthHeader.setAuthValue(doc.getTicket().getData());

        try {
            restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "403");
        }
    }

    @Test(groups = { "functional", "deployment" })
    public void deleteModelSummaryNoEditPlsModelsRight() {
        UserDocument doc = loginAndAttach("rgonzalez");
        addAuthHeader.setAuthValue(doc.getTicket().getData());

        try {
            restTemplate.delete(getRestAPIHostPort() + "/pls/modelsummaries/123");
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "403");
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = { "functional", "deployment" })
    public void getModelSummariesHasViewPlsModelsRight() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), ModelSummary.class);
        assertEquals(summary.getName(), "PLSModel-Eloqua-02/18/2015 07:25:38 GMT");
        assertNotNull(summary.getDetails());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "getModelSummariesHasViewPlsModelsRight" })
    public void updateModelSummaryHasEditPlsModelsRight() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Name", "xyz");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());

        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), ModelSummary.class);
        assertEquals(summary.getName(), "xyz");
        assertNotNull(summary.getDetails());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "updateModelSummaryHasEditPlsModelsRight" })
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
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), ModelSummary.class);
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
    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "updateAsDeletedModelSummaryHasEditPlsModelsRight" })
    public void deleteModelSummaryHasEditPlsModelsRight() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        restTemplate.delete(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"));
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort()  + "/pls/modelsummaries/ms-8e3a9d8c-3bc1-4d21-9c91-0af28afc5c9a", ModelSummary.class);
        assertNull(summary);
    }


    @Test(groups = { "functional", "deployment" })
    public void postModelSummariesNoCreatePlsModelsRight() {
        addAuthHeader.setAuthValue(loginAndAttachGeneral().getTicket().getData());
        try {
            InputStream ins = getClass().getClassLoader().getResourceAsStream("com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");
            assertNotNull(ins, "Testing json file is missing");
            ModelSummary modelSummary = ModelSummary.generateFromJSON(ins);
            restTemplate.postForObject(getRestAPIHostPort() + "/pls/modelsummaries/", modelSummary, Boolean.class);
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "403");
        }
    }

    @Test(groups = { "functional", "deployment" })
    public void postModelSummariesHasCreatePlsModelsRight() throws IOException {
        InputStream ins = getClass().getClassLoader().getResourceAsStream("com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");
        assertNotNull(ins, "Testing json file is missing");
        ModelSummary modelSummary = ModelSummary.generateFromJSON(ins);
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        int originalNumModels = response.size();

        try {
            boolean success = restTemplate.postForObject(getRestAPIHostPort() + "/pls/modelsummaries/", modelSummary, Boolean.class);
            assertTrue(success);
            response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
            assertNotNull(response);
            assertEquals(response.size(), originalNumModels + 1);
        } finally {
            restTemplate.delete(getRestAPIHostPort() + "/pls/modelsummaries/" + modelSummary.getId());
            response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
            assertEquals(response.size(), originalNumModels);
        }
    }
}
