package com.latticeengines.upgrade.pls;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.message.BasicNameValuePair;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class PlsGaManagerTestNG extends UpgradeFunctionalTestNGBase {

    @BeforeClass(groups = "functional")
    private void setup() { tearDown(); }

    @AfterClass(groups = "functional")
    public void tearDown() {
        String url = plsApiHost + "/pls/admin/tenants/" + TUPLE_ID;
        magicRestTemplate.delete(url);
        Assert.assertFalse(tenantExists(), "Test tenant should be deleted.");
    }

    @Test(groups = "functional")
    public void testRegisterTenant() {
        plsGaManager.registerTenant(CUSTOMER);
        Assert.assertTrue(tenantExists(), "Test tenant does not exists.");
    }

    @Test(groups = "functional", dependsOnMethods = "testRegisterTenant")
    public void testAssignSuperUsers() throws IOException {
        plsGaManager.setupAdminUsers(CUSTOMER);

        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair("Content-Type", "application/json; charset=utf-8"));
        headers.add(new BasicNameValuePair("Accept", "application/json"));

        String url = plsApiHost + "/pls/login";
        String payload = "{\"Username\":\"" + PLS_USRNAME + "\", \"Password\":\""
                + DigestUtils.sha256Hex(PLS_PASSWORD) + "\"}";
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(url, false, headers, payload);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(response);

        boolean seeTestTenant = false;
        for (JsonNode tenant: json.get("Result").get("Tenants")) {
            if (TUPLE_ID.equals(tenant.get("Identifier").asText())) {
                seeTestTenant = true;
                break;
            }
        }
        Assert.assertTrue(seeTestTenant, "Did not see the test tenant in the tenent list upon login PLS.");
    }

    @Test(groups = "functional", dependsOnMethods = "testAssignSuperUsers")
    public void testChangeModelName() {
        uploadModel();

        plsGaManager.updateModelName(MODEL_GUID, "New Name");

        List response = restTemplate.getForObject(plsApiHost + "/pls/modelsummaries/", List.class);
        ModelSummary summary = JsonUtils.deserialize(JsonUtils.serialize(response.get(0)), ModelSummary.class);
        Assert.assertEquals(summary.getName(), "New Name");
    }

    @Test(groups = "functional", dependsOnMethods = "testAssignSuperUsers")
    public void testSetModelActivity() throws IOException {
        List response = restTemplate.getForObject(plsApiHost + "/pls/modelsummaries/", List.class);
        ModelSummary summary = JsonUtils.deserialize(JsonUtils.serialize(response.get(0)), ModelSummary.class);
        Assert.assertEquals(summary.getStatus(), ModelSummaryStatus.INACTIVE);

        plsGaManager.setModelActivity(MODEL_GUID, true);

        response = restTemplate.getForObject(plsApiHost + "/pls/modelsummaries/", List.class);
        summary = JsonUtils.deserialize(JsonUtils.serialize(response.get(0)), ModelSummary.class);
        Assert.assertEquals(summary.getStatus(), ModelSummaryStatus.ACTIVE);
    }

    @Test(groups = "functional", dependsOnMethods = "testSetModelActivity")
    public void testGetModelIDs() {
        List<String> ids = plsGaManager.getModelIds(CUSTOMER);
        Assert.assertTrue(ids.contains(MODEL_GUID), "Should contain the testing modelID.");
    }
}
